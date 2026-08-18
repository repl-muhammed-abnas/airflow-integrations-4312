"""
Opportunity Sync DAG — D365 opportunity -> PIM Opportunity (classID=7).

Triggered by the router DAG with dag_run.conf containing:
  - entity_guid: the D365 opportunityid GUID

Custom API endpoints:
  - Opportunity.ashx (AddOpportunity / UpdateOpportunity)
  - EntityContacts.ashx (sync contacts after create/update)
  - ExternalIntegrationMapping.ashx (create/read mapping)
  - DropdownValues.ashx (create missing division/office/group)

Division/office/group/company are resolved from the opportunity's
vs360_segment entity.  If a division/office/group does not exist in PIM,
it is created via DropdownValues.ashx.  Company is read-only — omitted
if unmapped.  Status uses statecode (0=Active, 1=Inactive), resolved to
PIM status ID.

IMPORTANT: AddOpportunity FAILS without an organisation field.
           Must resolve ExternalOrg mapping for _vs360_primaryclientid_value
           before building the create body.

Flow
----
can_run_batch_task
  -> Yes: batch_task (create_log … sync_entity_contacts) >> catch_and_log_errors
  -> No:  create_log >> … >> sync_entity_contacts >> catch_and_log_errors
Inside batch: fetch_d365_opportunity >> check_has_segment
    -> Yes: fetch_d365_segment >> merge_ref_data_sources
    -> No: merge_ref_data_sources (uses direct entity fields)
  >> division >> office >> group >> company
  >> resolve_org >> get_entity_mapping >> is_opp_exists
    -> YES: update_opportunity -> converge
    -> NO:  create_opportunity -> add_mapping -> converge
  >> proposal_manager >> contacts
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
)
from associatedengineeringgroup.deltek_pim_d365_integration.utils.python_methods import (
    build_add_mapping_body,
    build_lead_contacts_body,
    build_opportunity_body,
    build_merged_ref_data,
    check_entity_field,
    check_mapping_exists,
    extract_custom_api_id,
    filter_mapping_by_type,
    get_unmapped_external_contacts,
    has_unmapped_external_contacts,
    resolve_opportunity_contacts,
    safe_json_response,
    validate_company_mapping,
    validate_ref_data_mapping,
)


def create_dag(config):
    """Create the Opportunity sync DAG for a given instance."""

    mapping_type = MAPPING_TYPE_NAMES['OPPORTUNITY']
    class_id = ENTITY_CLASS_IDS['OPPORTUNITY']

    with rail.create_airflow_dag(
        dag_id=config.opportunity_dag_id,
        description='Sync D365 Opportunity to PIM Opportunity',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['pim_d365', 'opportunity', 'sync'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        },
    ) as dag:

        # ── 1. Batch task toggle (default: enabled) ─────────────────
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'{config.opportunity_dag_id}_can_run_batch_task',
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

        # ── 2. View dag_run conf ──────────────────────────────────────
        view_conf = rail.ViewDagRunConfOperator(
            task_id='view_conf'
        )

        # ── 2b. Create log for sync tracking ─────────────────────────
        create_log = rail.CreateLogOperator(task_id='create_log')

        # ── 3. Fetch D365 Opportunity ─────────────────────────────────
        fetch_d365_opportunity = rail.SimpleHttpOperator(
            task_id='get_d365_entity',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}opportunities"
                "({{ dag_run.conf.entity_guid }})"
                "?$select=opportunityid,vs360_opportunitynumber,name,"
                "statecode,vs360_confidential,vs360_scope,"
                "vs360_totalprojectvalue,_vs360_primaryclientid_value,"
                "_ae_proposalmanager2_value,_vs360_segmentid_value,"
                "_vs360_marketid_value,_vs360_officeid_value,"
                "_vs360_company_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json(),
        )

        # ── 4. Fetch D365 Segment (Group/Profit Centre) ──────────────
        check_has_segment = rail.IfOperator(
            task_id='check_has_segment',
            test=check_entity_field('_vs360_segmentid_value'),
            yes_task='fetch_d365_segment',
            no_task='merge_ref_data_sources',
        )

        fetch_d365_segment = rail.SimpleHttpOperator(
            task_id='fetch_d365_segment',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_segments("
                "{{ result('get_d365_entity')['_vs360_segmentid_value'] }}"
                ")?$select=vs360_segmentid,vs360_name,"
                "_vs360_marketid_value,_ae_office_value,_ae_company_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json(),
        )

        # ── 4b. Merge ref-data sources (segment or direct entity) ─────
        merge_ref_data_sources = rail.PythonOperator(
            task_id='merge_ref_data_sources',
            trigger_rule='none_failed_min_one_success',
            python_callable=build_merged_ref_data(),
        )

        # ── 5. Division — resolve or create via process_udfs child DAG ──
        check_has_division = rail.IfOperator(
            task_id='check_has_division',
            test=lambda: bool(
                (rail.result('merge_ref_data_sources') or {})
                .get('_vs360_marketid_value')),
            yes_task='process_division_udf',
            no_task='check_has_office',
        )

        process_division_udf = rail.TriggerDagRunForEachItemOperator(
            task_id='process_division_udf',
            items=[1],
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('merge_ref_data_sources').get('_vs360_marketid_value'),
                'mapping_type_name': MAPPING_TYPE_NAMES['DIVISION'],
                'pim_add_function': 'AddDivision',
                'name': rail.result('merge_ref_data_sources').get(
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

        # ── 6. Office — resolve or create via process_udfs child DAG ──
        check_has_office = rail.IfOperator(
            task_id='check_has_office',
            test=lambda: bool(
                (rail.result('merge_ref_data_sources') or {})
                .get('_ae_office_value')),
            yes_task='process_office_udf',
            no_task='check_has_group',
        )

        process_office_udf = rail.TriggerDagRunForEachItemOperator(
            task_id='process_office_udf',
            items=[1],
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('merge_ref_data_sources').get('_ae_office_value'),
                'mapping_type_name': MAPPING_TYPE_NAMES['OFFICE'],
                'pim_add_function': 'AddOffice',
                'name': rail.result('merge_ref_data_sources').get(
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

        # ── 7. Group — resolve or create via process_udfs child DAG ───
        check_has_group = rail.IfOperator(
            task_id='check_has_group',
            test=check_entity_field('_vs360_segmentid_value'),
            yes_task='process_group_udf',
            no_task='check_has_company',
        )

        process_group_udf = rail.TriggerDagRunForEachItemOperator(
            task_id='process_group_udf',
            items=[1],
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('get_d365_entity').get('_vs360_segmentid_value'),
                'mapping_type_name': MAPPING_TYPE_NAMES['GROUP'],
                'pim_add_function': 'AddGroup',
                'name': rail.result('merge_ref_data_sources').get('vs360_name'),
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

        # ── 7b. Company mapping (required — error if missing) ─────────
        check_has_company = rail.IfOperator(
            task_id='check_has_company',
            test=lambda: bool(
                (rail.result('merge_ref_data_sources') or {})
                .get('_ae_company_value')),
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
                "&source={{ result('merge_ref_data_sources')['_ae_company_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=filter_mapping_by_type(MAPPING_TYPE_NAMES['COMPANY']),
        )

        check_company_mapping_exists = rail.IfOperator(
            task_id='check_company_mapping_exists',
            test=validate_company_mapping,
            yes_task='check_has_org',
            no_task='catch_and_log_errors',
        )

        # ── 8. Resolve primary client org mapping (create if missing) ─
        check_has_org = rail.IfOperator(
            task_id='check_has_org',
            test=check_entity_field('_vs360_primaryclientid_value'),
            yes_task='get_primary_client_mapping',
            no_task='get_entity_mapping',
        )

        resolve_org_mapping = rail.SimpleHttpOperator(
            task_id='get_primary_client_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_ORG'])}"
                "&source={{ result('get_d365_entity')"
                "['_vs360_primaryclientid_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response,
        )

        check_org_exists = rail.IfOperator(
            task_id='check_org_exists',
            test=lambda: validate_ref_data_mapping('get_primary_client_mapping'),
            yes_task='get_entity_mapping',
            no_task='trigger_external_org_sync',
        )

        # ── 8b. Delegate org creation to external_org_sync DAG ────────
        trigger_external_org_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_external_org_sync',
            items=[1],
            trigger_dag_id=config.external_org_dag_id,
            conf=lambda: {
                'entity_guid': rail.result('get_d365_entity')['_vs360_primaryclientid_value'],
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

        fetch_org_mapping_after_sync = rail.SimpleHttpOperator(
            task_id='fetch_org_mapping_after_sync',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                f"&name={quote(MAPPING_TYPE_NAMES['EXTERNAL_ORG'])}"
                "&source={{ result('get_d365_entity')"
                "['_vs360_primaryclientid_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=safe_json_response,
        )

        # ── 9. Get Opportunity mapping ────────────────────────────────
        get_opp_mapping = rail.SimpleHttpOperator(
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

        # ── 10. Branch: create or update ──────────────────────────────
        is_opp_exists = rail.IfOperator(
            task_id='is_opp_exists',
            test=check_mapping_exists,
            yes_task='build_update_body',
            no_task='build_create_body',
        )

        # ── 11a. UPDATE branch ────────────────────────────────────────
        build_update_body = rail.PythonOperator(
            task_id='build_update_body',
            python_callable=build_opportunity_body('update'),
        )

        update_opportunity = rail.SimpleHttpOperator(
            task_id='update_opportunity',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['OPPORTUNITY']}"
                '?function=UpdateOpportunity'
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_update_body') }}",
            response_filter=safe_json_response,
        )

        # ── 11b. CREATE branch ────────────────────────────────────────
        build_create_body = rail.PythonOperator(
            task_id='build_create_body',
            python_callable=build_opportunity_body('create'),
        )

        create_opportunity = rail.SimpleHttpOperator(
            task_id='create_entity',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['OPPORTUNITY']}"
                '?function=AddOpportunity'
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_create_body') }}",
            response_filter=extract_custom_api_id,
        )

        # ── 12. Add mapping after create ──────────────────────────────
        prepare_mapping_body = rail.PythonOperator(
            task_id='prepare_mapping_body',
            python_callable=build_add_mapping_body(mapping_type),
        )

        add_opp_mapping = rail.SimpleHttpOperator(
            task_id='add_opp_mapping',
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

        # ── 13. Convergence point ─────────────────────────────────────
        converge = rail.PythonOperator(
            task_id='converge',
            trigger_rule='none_failed_min_one_success',
            python_callable=lambda: True,
        )

        # ── 14. Proposal manager mapping (internal contact) ───────────
        check_pm_assigned = rail.IfOperator(
            task_id='check_pm_assigned',
            test=check_entity_field('_ae_proposalmanager2_value'),
            yes_task='resolve_pm_employee',
            no_task='fetch_opp_contacts',
        )

        resolve_pm_employee = rail.SimpleHttpOperator(
            task_id='resolve_pm_employee',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_employees"
                "?$filter=_vs360_systemuserid_value eq "
                "'{{ result('get_d365_entity')['_ae_proposalmanager2_value'] }}'"
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
            no_task='fetch_opp_contacts',
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
            yes_task='fetch_opp_contacts',
            no_task='trigger_pm_sync',
        )

        trigger_pm_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_pm_sync',
            trigger_dag_id=config.internal_contact_dag_id,
            items=lambda: [rail.result('resolve_pm_employee')],
            conf=lambda item: {'entity_guid': item, 'triggered_by': rail.get_current_context()['dag_run'].run_id},
        )

        wait_for_pm_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_pm_sync',
            dag_runs="{{ result('trigger_pm_sync') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
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

        # ── 15. Contacts sync ─────────────────────────────────────────
        fetch_opp_contacts = rail.SimpleHttpOperator(
            task_id='fetch_opp_contacts',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_opportunitycontacts"
                "?$filter=_vs360_opportunityid_value eq "
                "'{{ dag_run.conf.entity_guid }}'"
                "&$select=vs360_opportunitycontactid,"
                "_vs360_contactid_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json().get('value', []),
        )

        fetch_opp_team = rail.SimpleHttpOperator(
            task_id='fetch_opp_team',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{D365_API_VERSION}vs360_opportunityteams"
                "?$filter=_vs360_opportunityid_value eq "
                "'{{ dag_run.conf.entity_guid }}'"
                "&$select=vs360_opportunityteamid,"
                "_vs360_employeeid_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **D365_ODATA_HEADERS,
            },
            response_filter=lambda r: r.json().get('value', []),
        )

        # ── 15b. Pre-sync unmapped external contacts ─────────────────
        check_unmapped_ext_contacts = rail.PythonOperator(
            task_id='get_unmapped_external_contacts',
            python_callable=get_unmapped_external_contacts(
                config.pim_conn_id, config.instance,
                contacts_task_id='fetch_opp_contacts',
            ),
        )

        has_unmapped_ext = rail.IfOperator(
            task_id='check_unmapped_ext_contacts',
            test=has_unmapped_external_contacts,
            yes_task='trigger_external_contact_sync',
            no_task='get_entity_contacts',
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

        resolve_contacts = rail.PythonOperator(
            task_id='get_entity_contacts',
            python_callable=resolve_opportunity_contacts(
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

        # ── 16. Logging ───────────────────────────────────────────────
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'entity_type': 'Opportunity',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ result('get_d365_entity').get('name', '') if result('get_d365_entity') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
        )

        # ── 17. Trigger global error log DAG on failure ───────────────
        trigger_error_log = rail.TriggerDagRunOperator(
            task_id='trigger_error_log',
            trigger_dag_id=config.error_log_dag_id,
            conf={
                'entity_type': 'Opportunity',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ result('get_d365_entity').get('name', '') if result('get_d365_entity') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
            wait_for_completion=False,
        )

        # ── Task wiring (sequential for BatchTaskRunOperator) ─────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> fetch_d365_opportunity >> check_has_segment

        # Segment branch — merge ref-data sources from segment or direct fields
        (
            check_has_segment >> rail.Label('Has segment') >>
            fetch_d365_segment >> merge_ref_data_sources
        )
        (
            check_has_segment >> rail.Label('No segment') >>
            merge_ref_data_sources
        )
        merge_ref_data_sources >> check_has_division

        # Division — trigger process_udfs child DAG
        (
            check_has_division >> rail.Label('Yes') >>
            process_division_udf >> wait_for_division_udf >>
            get_pim_division_id >> check_has_office
        )
        (
            check_has_division >> rail.Label('No') >>
            check_has_office
        )

        # Office — trigger process_udfs child DAG
        (
            check_has_office >> rail.Label('Yes') >>
            process_office_udf >> wait_for_office_udf >>
            get_pim_office_id >> check_has_group
        )
        (
            check_has_office >> rail.Label('No') >>
            check_has_group
        )

        # Group — trigger process_udfs child DAG
        (
            check_has_group >> rail.Label('Yes') >>
            process_group_udf >> wait_for_group_udf >>
            get_pim_group_id >> check_has_company
        )
        (
            check_has_group >> rail.Label('No') >>
            check_has_company
        )

        # Company mapping (required — error if missing)
        (
            check_has_company >> rail.Label('Has company') >>
            get_company_mapping >> check_company_mapping_exists
        )
        check_company_mapping_exists >> rail.Label('Company mapped') >> check_has_org
        check_company_mapping_exists >> rail.Label('Company not mapped') >> catch_and_log_errors
        check_has_company >> rail.Label('No company') >> catch_and_log_errors

        # Primary client org resolve-or-create (with null guard)
        (
            check_has_org >> rail.Label('Has org') >>
            resolve_org_mapping >> check_org_exists
        )
        (
            check_has_org >> rail.Label('No org') >>
            get_opp_mapping
        )
        (
            check_org_exists >> rail.Label('Org exists') >>
            get_opp_mapping
        )
        (
            check_org_exists >> rail.Label('Create org') >>
            trigger_external_org_sync >> wait_for_external_org_sync >>
            fetch_org_mapping_after_sync >> get_opp_mapping
        )

        get_opp_mapping >> is_opp_exists

        # Update branch
        (
            is_opp_exists
            >> rail.Label('Opportunity exists in PIM')
            >> build_update_body
            >> update_opportunity
            >> converge
        )

        # Create branch
        (
            is_opp_exists
            >> rail.Label('New opportunity')
            >> build_create_body
            >> create_opportunity
            >> prepare_mapping_body
            >> add_opp_mapping
            >> converge
        )

        # PM resolution chain
        converge >> check_pm_assigned
        (
            check_pm_assigned >> rail.Label('PM assigned') >>
            resolve_pm_employee >> check_pm_employee_found
        )
        (
            check_pm_assigned >> rail.Label('No PM') >>
            fetch_opp_contacts
        )
        (
            check_pm_employee_found >> rail.Label('Employee found') >>
            get_pm_mapping >> check_pm_exists
        )
        (
            check_pm_employee_found >> rail.Label('No employee') >>
            fetch_opp_contacts
        )
        (
            check_pm_exists >> rail.Label('PM exists') >>
            fetch_opp_contacts
        )
        (
            check_pm_exists >> rail.Label('Sync PM') >>
            trigger_pm_sync >> wait_for_pm_sync >>
            get_pm_mapping_after_sync >> fetch_opp_contacts
        )

        # External contact pre-sync + contacts sync
        (
            fetch_opp_contacts
            >> fetch_opp_team
            >> check_unmapped_ext_contacts
            >> has_unmapped_ext
        )
        (
            has_unmapped_ext >> rail.Label('Yes') >>
            trigger_ext_contact_sync >> wait_for_ext_contact_sync >>
            resolve_contacts
        )
        (
            has_unmapped_ext >> rail.Label('No') >>
            resolve_contacts
        )
        (
            resolve_contacts
            >> prepare_contacts_body
            >> sync_entity_contacts
            >> catch_and_log_errors
        )

        # Post-sync: error notification (outside the batch)
        catch_and_log_errors >> trigger_error_log

        return dag


rail.for_each_instance(create_dag)
