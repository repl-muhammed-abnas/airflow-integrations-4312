import json
import rail

from datetime import timedelta
from airflow.models import Variable
from associatedengineeringgroup.deltek_pim_d365_integration.utils import python_methods, request_payload
from urllib.parse import quote


def create_dag(config):
    """Create the project sync DAG for a given instance."""

    mapping_type = config.MAPPING_TYPE_NAMES['PROJECT']

    with rail.create_airflow_dag(
        dag_id=config.project_dag_id,
        description=f'D365 to PIM project sync {config.instance}',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['pim_d365', 'project', 'sync'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        },
    ) as dag:

        # ── 1. Batch task toggle (default: enabled) ─────────────────
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'{config.project_dag_id}_can_run_batch_task',
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
        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_conf'
        )

        # ── 2b. Create log for sync tracking ─────────────────────────
        create_log = rail.CreateLogOperator(task_id='create_log')

        # Fetches the D365 project task with its expanded project details via OData
        get_d365_project = rail.SimpleHttpOperator(
            task_id='get_d365_project',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{config.D365_API_VERSION}msdyn_projecttasks"
                "({{ dag_run.conf.entity_guid }})"
                "?$select=msdyn_projecttaskid,vs360_projectid,msdyn_subject,"
                "ae_projectstatus,msdyn_start,msdyn_finish,vs360_totalcontract,"
                "msdyn_outlinelevel,_vs360_segment_value,_vs360_company_value,"
                "_msdyn_project_value"
                "&$expand=msdyn_project($select=msdyn_projectid,ae_projectdescription,"
                "_ae_division_value,_vs360_office_value,_vs360_segment_value,"
                "_vs360_company_value,_msdyn_customer_value,_ae_contact_value,"
                "_ae_projectowner2_value)"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **config.D365_ODATA_HEADERS,
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the full D365 project task dict from the raw HTTP response
        parse_get_d365_project = rail.PythonOperator(
            task_id='parse_get_d365_project',
            python_callable=lambda: rail.result('get_d365_project'),
        )

        # Only top-level project tasks (outline level 1) are synced
        if_outline_level_is_equal_to_1 = rail.IfOperator(
            task_id='if_outline_level_is_equal_to_1',
            test=lambda: rail.result('parse_get_d365_project').get('msdyn_outlinelevel') == 1,
            yes_task='if_project_has_company',
            no_task='stop_due_to_outline_level_not_1',
        )

        # Not a top-level project task; skip processing
        stop_due_to_outline_level_not_1 = rail.EmptyOperator(
            task_id='stop_due_to_outline_level_not_1',
        )

        # Ensures the project has a linked company before proceeding; company is required for PIM sync
        if_project_has_company = rail.IfOperator(
            task_id='if_project_has_company',
            test=lambda: python_methods.check_if_field_is_truthy(
                d365_entity=rail.result('parse_get_d365_project'),
                field_name='_vs360_company_value'),
            yes_task='get_company_mapping',
            no_task='stop_due_to_no_company_associated',
        )

        # Halts processing; project cannot sync to PIM without a company
        stop_due_to_no_company_associated = rail.EmptyOperator(
            task_id='stop_due_to_no_company_associated',
        )

        # Looks up the PIM company ID mapped to the D365 company GUID via ExternalIntegrationMapping
        get_company_mapping = rail.SimpleHttpOperator(
            task_id='get_company_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                "&source={{ result('parse_get_d365_project')['_vs360_company_value'] }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=python_methods.filter_mapping_by_type(
                config.MAPPING_TYPE_NAMES['COMPANY']),
        )

        # Extracts the PIM company destinationId from the mapping API response
        parse_get_company_mapping = rail.PythonOperator(
            task_id='parse_get_company_mapping',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('get_company_mapping'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['COMPANY'],
            ),
        )

        # Stops sync if the company is not in the mapping
        if_company_mapping_not_found = rail.IfOperator(
            task_id='if_company_mapping_not_found',
            test=lambda: not bool(rail.result('parse_get_company_mapping')),
            yes_task='stop_due_to_no_company_mapping',
            no_task='get_pim_project_id_from_project_mapper',
        )

        # Halts processing; company must be synced to PIM before this project can proceed
        stop_due_to_no_company_mapping = rail.EmptyOperator(
            task_id='stop_due_to_no_company_mapping',
        )

        # Checks whether this D365 project task already has a PIM project via ExternalIntegrationMapping
        get_pim_project_id_from_project_mapper = rail.SimpleHttpOperator(
            task_id='get_pim_project_id_from_project_mapper',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                "&source={{ dag_run.conf.entity_guid }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the existing PIM project destinationId from the mapping API response
        parse_get_pim_project_id_from_project_mapper = rail.PythonOperator(
            task_id='parse_get_pim_project_id_from_project_mapper',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('get_pim_project_id_from_project_mapper'),
                key='destinationId',
                type_name=mapping_type,
            ),
        )

        # Branches to update an existing PIM project or create a new one
        if_pim_project_id_exists_in_project_mapper = rail.IfOperator(
            task_id='if_pim_project_id_exists_in_project_mapper',
            test=lambda: bool(rail.result('parse_get_pim_project_id_from_project_mapper')),
            yes_task='build_update_project_in_pim_body',
            no_task='build_create_project_in_pim_body',
        )

        # Builds the JSON payload for updating an existing PIM project
        build_update_project_in_pim_body = rail.PythonOperator(
            task_id='build_update_project_in_pim_body',
            python_callable=lambda: request_payload.build_project_create_or_update_request_body(
                config=config,
                d365_project=rail.result('parse_get_d365_project'),
            ),
        )

        # Updates the existing PIM project with the latest D365 field values
        update_project_in_pim = rail.SimpleHttpOperator(
            task_id='update_project_in_pim',
            method='PUT',
            http_conn_id=config.pim_conn_id,
            endpoint=f"{config.PIM_STANDARD_API_BASE}projects/{{{{ result('parse_get_pim_project_id_from_project_mapper') }}}}",
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_update_project_in_pim_body') }}",
        )

        # Builds the JSON payload for creating a new PIM project
        build_create_project_in_pim_body = rail.PythonOperator(
            task_id='build_create_project_in_pim_body',
            python_callable=lambda: request_payload.build_project_create_or_update_request_body(
                config=config,
                d365_project=rail.result('parse_get_d365_project'),
            ),
        )

        # Creates a new PIM project from the D365 project task data; returns the new PIM project ID
        create_project_in_pim = rail.SimpleHttpOperator(
            task_id='create_project_in_pim',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=f'{config.PIM_STANDARD_API_BASE}projects',
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_create_project_in_pim_body') }}",
            response_filter=lambda response: response.json(),
        )

        # Extracts the newly created PIM project ID from the create response
        parse_create_project_in_pim = rail.PythonOperator(
            task_id='parse_create_project_in_pim',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('create_project_in_pim'),
                key='id',
            ),
        )

        # Builds the ExternalIntegrationMapping registration body linking D365 GUID to new PIM project ID
        build_add_project_mapping_body = rail.PythonOperator(
            task_id='build_add_project_mapping_body',
            python_callable=lambda: json.dumps({
                "name": mapping_type,
                "sourceGuid": rail.result('parse_get_d365_project').get('msdyn_projecttaskid'),
                "destinationId": rail.result('parse_create_project_in_pim'),
            }),
        )

        # Registers the new PIM project ID in ExternalIntegrationMapping so future syncs update rather than create
        add_project_mapping = rail.SimpleHttpOperator(
            task_id='add_project_mapping',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=AddMapping"
                f"&name={quote(mapping_type)}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_add_project_mapping_body') }}",
        )

        # Checks if the D365 project has a division field set
        if_division_present_in_get_d365_project = rail.IfOperator(
            task_id='if_division_present_in_get_d365_project',
            test=lambda: python_methods.check_if_field_is_truthy(
                d365_entity=rail.result('parse_get_d365_project').get('msdyn_project'),
                field_name='_ae_division_value') if rail.result('parse_get_d365_project').get('msdyn_project') else False,
            yes_task='process_division_udf',
            no_task='if_office_present_in_get_d365_project',
        )

        # Triggers the generic UDF DAG to resolve or create the division in PIM DropdownValues
        process_division_udf = rail.TriggerDagRunOperator(
            task_id='process_division_udf',
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('parse_get_d365_project').get('msdyn_project').get('_ae_division_value'),
                'mapping_type_name': config.MAPPING_TYPE_NAMES['DIVISION'],
                'pim_add_function': 'AddDivision',
                'name': rail.result('parse_get_d365_project').get('msdyn_project').get(f'_ae_division_value{config.D365_FORMATTED_VALUE}'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            wait_for_completion=False,
        )

        wait_for_division_udf = rail.WaitForDagRunsSensor(
            task_id='wait_for_division_udf',
            dag_runs="{{ result('process_division_udf') }}",
        )

        # Collects the resolved PIM division ID from the child UDF DAG run
        get_pim_division_id = rail.GatherResultsFromDagRunsOperator(
            task_id='get_pim_division_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_division_udf') }}",
            dagrun_task_id='get_destination_id',
            flatten=True,
        )

        # Checks if the D365 project has an office field set; skips UDF update if absent
        if_office_present_in_get_d365_project = rail.IfOperator(
            task_id='if_office_present_in_get_d365_project',
            test=lambda: python_methods.check_if_field_is_truthy(
                d365_entity=rail.result('parse_get_d365_project').get('msdyn_project'),
                field_name='_vs360_office_value') if rail.result('parse_get_d365_project').get('msdyn_project') else False,
            yes_task='process_office_udf',
            no_task='if_segment_present_in_get_d365_project',
        )

        # Triggers the generic UDF DAG to resolve or create the office in PIM DropdownValues
        process_office_udf = rail.TriggerDagRunOperator(
            task_id='process_office_udf',
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('parse_get_d365_project').get('msdyn_project').get('_vs360_office_value'),
                'mapping_type_name': config.MAPPING_TYPE_NAMES['OFFICE'],
                'pim_add_function': 'AddOffice',
                'name': rail.result('parse_get_d365_project').get('msdyn_project').get(f'_vs360_office_value{config.D365_FORMATTED_VALUE}'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            wait_for_completion=False,
        )

        wait_for_office_udf = rail.WaitForDagRunsSensor(
            task_id='wait_for_office_udf',
            dag_runs="{{ result('process_office_udf') }}",
        )

        # Collects the resolved PIM office ID from the child UDF DAG run
        get_pim_office_id = rail.GatherResultsFromDagRunsOperator(
            task_id='get_pim_office_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_office_udf') }}",
            dagrun_task_id='get_destination_id',
            flatten=True,
        )

        # Checks if the D365 project has a segment (group) field set; skips UDF update if absent
        if_segment_present_in_get_d365_project = rail.IfOperator(
            task_id='if_segment_present_in_get_d365_project',
            test=lambda: python_methods.check_if_field_is_truthy(
                d365_entity=rail.result('parse_get_d365_project').get('msdyn_project'),
                field_name='_vs360_segment_value') if rail.result('parse_get_d365_project').get('msdyn_project') else False,
            yes_task='process_segment_udf',
            no_task='build_update_project_udf_body',
        )

        # Triggers the generic UDF DAG to resolve or create the group in PIM DropdownValues
        process_segment_udf = rail.TriggerDagRunOperator(
            task_id='process_segment_udf',
            trigger_dag_id=config.process_udfs_dag_id,
            conf=lambda: {
                'source_guid': rail.result('parse_get_d365_project').get('msdyn_project').get('_vs360_segment_value'),
                'mapping_type_name': config.MAPPING_TYPE_NAMES['GROUP'],
                'pim_add_function': 'AddGroup',
                'name': rail.result('parse_get_d365_project').get('msdyn_project').get(f'_vs360_segment_value{config.D365_FORMATTED_VALUE}'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            wait_for_completion=False,
        )

        wait_for_segment_udf = rail.WaitForDagRunsSensor(
            task_id='wait_for_segment_udf',
            dag_runs="{{ result('process_segment_udf') }}",
        )

        # Collects the resolved PIM group ID from the child UDF DAG run
        get_pim_group_id = rail.GatherResultsFromDagRunsOperator(
            task_id='get_pim_group_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_segment_udf') }}",
            dagrun_task_id='get_destination_id',
            flatten=True,
        )

        # Builds the UDF payload with division, office, group, dates, and company data
        build_update_project_udf_body = rail.PythonOperator(
            task_id='build_update_project_udf_body',
            python_callable=lambda: request_payload.build_project_udf_update_request_body(
                d365_project=rail.result('parse_get_d365_project'),
                pim_division_id=rail.result('get_pim_division_id'),
                pim_office_id=rail.result('get_pim_office_id'),
                pim_group_id=rail.result('get_pim_group_id'),
            ),
        )

        # Posts division, office, group, dates, and company UDFs to PIM via the Project custom API
        update_project_udf = rail.SimpleHttpOperator(
            task_id='update_project_udf',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['PROJECT']}"
                f"?function=UpdateProjectUDF"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_update_project_udf_body') }}",
        )

        # Checks if the project has a linked customer (external organization)
        if_msdyn_customer_truthy_in_get_d365_project = rail.IfOperator(
            task_id='if_msdyn_customer_truthy_in_get_d365_project',
            test=lambda: bool(rail.result('parse_get_d365_project').get('msdyn_project').get('_msdyn_customer_value')) if rail.result('parse_get_d365_project') and rail.result('parse_get_d365_project').get('msdyn_project') else False,
            yes_task='get_pim_external_organization_id_from_mapper',
            no_task='if_ae_contact_value_truthy_in_get_d365_project',
        )

        # Looks up the PIM external organization mapped to the D365 customer account
        get_pim_external_organization_id_from_mapper = rail.SimpleHttpOperator(
            task_id='get_pim_external_organization_id_from_mapper',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                "&source={{ result('parse_get_d365_project').get('msdyn_project').get('_msdyn_customer_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the PIM external organization destinationId from the mapping response
        parse_get_pim_external_organization_id_from_mapper = rail.PythonOperator(
            task_id='parse_get_pim_external_organization_id_from_mapper',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('get_pim_external_organization_id_from_mapper'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['EXTERNAL_ORG'],
            ),
        )

        # Branches based on whether the external organization already has a PIM mapping
        if_pim_external_organization_id_exists_in_mapper = rail.IfOperator(
            task_id='if_pim_external_organization_id_exists_in_mapper',
            test=lambda: bool(rail.result('parse_get_pim_external_organization_id_from_mapper')),
            yes_task='get_existing_organizations_for_the_project',
            no_task='process_external_organization',
        )

        # Fetches the list of organizations currently linked to this PIM project
        get_existing_organizations_for_the_project = rail.SimpleHttpOperator(
            task_id='get_existing_organizations_for_the_project',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=f"{config.PIM_STANDARD_API_BASE}project/{{{{ result('parse_get_pim_project_id_from_project_mapper') or result('parse_create_project_in_pim') }}}}/organizations",
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            response_filter=lambda response: response.json()
        )

        # Checks if the customer org is already linked to the project with the correct role; skips link if already present
        if_external_organization_associated_with_the_project_with_specific_role = rail.IfOperator(
            task_id='if_external_organization_associated_with_the_project_with_specific_role',
            test=lambda: python_methods.is_external_organization_associated_with_project(config),
            yes_task='if_ae_contact_value_truthy_in_get_d365_project',
            no_task='process_external_organization',
        )

        # Triggers the external organization sync child DAG to create or update the org in PIM
        process_external_organization = rail.TriggerDagRunOperator(
            task_id='process_external_organization',
            trigger_dag_id=config.external_org_dag_id,
            conf=lambda: {
                'entity_guid': rail.result('parse_get_d365_project').get('msdyn_project').get('_msdyn_customer_value'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            wait_for_completion=False,
        )

        wait_for_external_organization = rail.WaitForDagRunsSensor(
            task_id='wait_for_external_organization',
            dag_runs="{{ result('process_external_organization') }}",
        )

        # Re-fetches the org mapping after the child DAG has created it
        get_pim_external_organization_id_from_mapper_after_creation = rail.SimpleHttpOperator(
            task_id='get_pim_external_organization_id_from_mapper_after_creation',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                "&source={{ result('parse_get_d365_project').get('msdyn_project').get('_msdyn_customer_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the PIM external organization destinationId after creation
        parse_get_pim_external_organization_id_from_mapper_after_creation = rail.PythonOperator(
            task_id='parse_get_pim_external_organization_id_from_mapper_after_creation',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('get_pim_external_organization_id_from_mapper_after_creation'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['EXTERNAL_ORG'],
            ),
        )

        # Builds the payload for linking the external organization to the PIM project with the configured role
        build_link_external_organization_to_the_pim_project_body = rail.PythonOperator(
            task_id='build_link_external_organization_to_the_pim_project_body',
            python_callable=lambda: request_payload.link_external_organization_to_the_pim_project_payload(config),
        )

        # Links the external organization to the PIM project with the configured role ID
        link_external_organization_to_the_pim_project = rail.SimpleHttpOperator(
            task_id='link_external_organization_to_the_pim_project',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=f"{config.PIM_STANDARD_API_BASE}project/{{{{ result('parse_get_pim_project_id_from_project_mapper') or result('parse_create_project_in_pim') }}}}/organizations",
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_link_external_organization_to_the_pim_project_body') }}",
            response_filter=lambda response: response.json()
        )

        # Checks if the D365 project has a designated primary contact (_ae_contact_value)
        if_ae_contact_value_truthy_in_get_d365_project = rail.IfOperator(
            task_id='if_ae_contact_value_truthy_in_get_d365_project',
            test=lambda: python_methods.check_if_field_is_truthy(
                d365_entity=rail.result('parse_get_d365_project').get('msdyn_project'),
                field_name='_ae_contact_value') if rail.result('parse_get_d365_project').get('msdyn_project') else False,
            yes_task='get_pim_external_contact_id_from_mapper',
            no_task='get_external_contacts_for_project_from_d365',
        )

        # Looks up the PIM contact ID for the primary ae_contact via ExternalIntegrationMapping
        get_pim_external_contact_id_from_mapper = rail.SimpleHttpOperator(
            task_id='get_pim_external_contact_id_from_mapper',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                "&source={{ result('parse_get_d365_project').get('msdyn_project').get('_ae_contact_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the primary ae_contact PIM destinationId from the mapping response
        parse_get_pim_external_contact_id_from_mapper = rail.PythonOperator(
            task_id='parse_get_pim_external_contact_id_from_mapper',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('get_pim_external_contact_id_from_mapper'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'],
            ),
        )

        # Branches based on whether the primary ae_contact already has a PIM mapping
        if_external_contact_for_ae_contact_exist_in_mapper = rail.IfOperator(
            task_id='if_external_contact_for_ae_contact_exist_in_mapper',
            test=lambda: bool(rail.result('parse_get_pim_external_contact_id_from_mapper')),
            yes_task='get_external_contacts_for_project_from_d365',
            no_task='process_external_contact_for_ae_contact_in_d365',
        )

        # Triggers the external contact sync child DAG to create the primary contact in PIM
        process_external_contact_for_ae_contact_in_d365 = rail.TriggerDagRunOperator(
            task_id='process_external_contact_for_ae_contact_in_d365',
            trigger_dag_id=config.external_contact_dag_id,
            conf=lambda: {
                'entity_guid': rail.result('parse_get_d365_project').get('msdyn_project').get('_ae_contact_value'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
            wait_for_completion=False,
        )

        wait_for_external_contact_for_ae_contact = rail.WaitForDagRunsSensor(
            task_id='wait_for_external_contact_for_ae_contact',
            dag_runs="{{ result('process_external_contact_for_ae_contact_in_d365') }}",
        )

        # Re-fetches the primary contact PIM mapping after the child DAG has created it
        get_pim_external_contact_id_from_mapper_after_creation = rail.SimpleHttpOperator(
            task_id='get_pim_external_contact_id_from_mapper_after_creation',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                "?function=GetMapping"
                "&source={{ result('parse_get_d365_project').get('msdyn_project').get('_ae_contact_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the primary ae_contact PIM destinationId after creation
        parse_get_pim_external_contact_id_from_mapper_after_creation = rail.PythonOperator(
            task_id='parse_get_pim_external_contact_id_from_mapper_after_creation',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('get_pim_external_contact_id_from_mapper_after_creation'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'],
            ),
        )

        # Fetches all external contacts linked to this D365 project from ae_projectcontacts table
        get_external_contacts_for_project_from_d365 = rail.SimpleHttpOperator(
            task_id='get_external_contacts_for_project_from_d365',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{config.D365_API_VERSION}ae_projectcontacts"
                "?$filter=_new_projects_value eq "
                "'{{ result('parse_get_d365_project').get('_msdyn_project_value') }}'"
                "&$select=ae_projectcontactid,_new_contact_value,_new_projects_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **config.D365_ODATA_HEADERS,
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the value array from the OData collection response for external contacts
        parse_get_external_contacts_for_project_from_d365 = rail.PythonOperator(
            task_id='parse_get_external_contacts_for_project_from_d365',
            python_callable=lambda: json.dumps((rail.result('get_external_contacts_for_project_from_d365') or {}).get('value', [])),
        )

        # ── Phase 1: Check existing external contact mappings, split mapped / needs-sync ──
        init_project_contact_list = rail.SetVariableOperator(
            task_id='init_project_contact_list',
            name='project_contact_list',
            value=[],
        )

        init_external_contacts_needs_sync = rail.SetVariableOperator(
            task_id='init_external_contacts_needs_sync',
            name='external_contacts_needs_sync',
            value=[],
        )

        check_contact_mapping_pre_filter = rail.SimpleHttpOperator(
            task_id='check_contact_mapping_pre_filter',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=GetMapping"
                "&source={{ result('for_each_pre_filter_external_contacts').get('_new_contact_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        parse_check_contact_mapping_pre_filter = rail.PythonOperator(
            task_id='parse_check_contact_mapping_pre_filter',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('check_contact_mapping_pre_filter'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'],
            ),
        )

        # Already-mapped contact appended directly; unmapped contact queued for sync
        if_external_contact_mapped = rail.IfOperator(
            task_id='if_external_contact_mapped',
            test=lambda: bool(rail.result('parse_check_contact_mapping_pre_filter')),
            yes_task='append_external_contact_to_project_contact_list',
            no_task='record_external_contact_needs_sync',
        )

        append_external_contact_to_project_contact_list = rail.SetVariableOperator(
            task_id='append_external_contact_to_project_contact_list',
            name='project_contact_list',
            append=True,
            value=lambda: {
                'd365_contact_guid': rail.result('for_each_pre_filter_external_contacts').get('_new_contact_value'),
                'pim_contact_id': rail.result('parse_check_contact_mapping_pre_filter'),
            },
        )

        record_external_contact_needs_sync = rail.SetVariableOperator(
            task_id='record_external_contact_needs_sync',
            name='external_contacts_needs_sync',
            append=True,
            value=lambda: rail.result('for_each_pre_filter_external_contacts'),
        )

        for_each_pre_filter_external_contacts = rail.ForEachOperator(
            task_id='for_each_pre_filter_external_contacts',
            items='{{ result("parse_get_external_contacts_for_project_from_d365") }}',
            start_task='check_contact_mapping_pre_filter',
            end_task='for_each_pre_filter_external_contacts_end',
        )

        for_each_pre_filter_external_contacts_end = rail.EmptyOperator(
            task_id='for_each_pre_filter_external_contacts_end',
        )

        # ── Trigger sync only for unmapped external contacts ──────────────────
        get_external_contacts_needs_sync = rail.GetVariableOperator(
            task_id='get_external_contacts_needs_sync',
            name='external_contacts_needs_sync',
        )

        parse_external_contacts_needs_sync = rail.PythonOperator(
            task_id='parse_external_contacts_needs_sync',
            python_callable=lambda: json.dumps((rail.result('get_external_contacts_needs_sync') or {}).get('value', [])),
        )

        trigger_pre_sync_external_contacts = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_pre_sync_external_contacts',
            items='{{ result("parse_external_contacts_needs_sync") }}',
            trigger_dag_id=config.external_contact_dag_id,
            conf=lambda item: {
                'entity_guid': item.get('_new_contact_value'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
        )

        wait_for_pre_sync_external_contacts = rail.WaitForDagRunsSensor(
            task_id='wait_for_pre_sync_external_contacts',
            dag_runs='{{ result("trigger_pre_sync_external_contacts") }}',
        )

        # ── Phase 2: Append newly-synced external contacts to project_contact_list ──
        check_new_contact_mapping = rail.SimpleHttpOperator(
            task_id='check_new_contact_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=GetMapping"
                "&source={{ result('for_each_new_external_contacts').get('_new_contact_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        parse_new_contact_mapping = rail.PythonOperator(
            task_id='parse_new_contact_mapping',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('check_new_contact_mapping'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'],
            ),
        )

        append_new_external_contact_to_project_contact_list = rail.SetVariableOperator(
            task_id='append_new_external_contact_to_project_contact_list',
            name='project_contact_list',
            append=True,
            value=lambda: {
                'd365_contact_guid': rail.result('for_each_new_external_contacts').get('_new_contact_value'),
                'pim_contact_id': rail.result('parse_new_contact_mapping'),
            },
        )

        for_each_new_external_contacts = rail.ForEachOperator(
            task_id='for_each_new_external_contacts',
            items='{{ result("parse_external_contacts_needs_sync") }}',
            start_task='check_new_contact_mapping',
            end_task='for_each_new_external_contacts_end',
        )

        for_each_new_external_contacts_end = rail.EmptyOperator(
            task_id='for_each_new_external_contacts_end',
        )

        # Fetches all internal contacts (employees) on this project from D365 ae_aeprojectteams table
        get_internal_contacts_for_project_from_d365 = rail.SimpleHttpOperator(
            task_id='get_internal_contacts_for_project_from_d365',
            method='GET',
            http_conn_id=config.d365_conn_id,
            endpoint=(
                f"{config.D365_API_VERSION}ae_aeprojectteams"
                "?$filter=_ae_projectid_value eq "
                "'{{ result('parse_get_d365_project').get('_msdyn_project_value') }}'"
                "&$select=ae_aeprojectteamid,_ae_employeeid_value,_ae_projectid_value"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.D365_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                **config.D365_ODATA_HEADERS,
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the value array from the OData collection response for internal contacts
        parse_get_internal_contacts_for_project_from_d365 = rail.PythonOperator(
            task_id='parse_get_internal_contacts_for_project_from_d365',
            python_callable=lambda: json.dumps((rail.result('get_internal_contacts_for_project_from_d365') or {}).get('value', [])),
        )

        # ── Phase 1: Check existing internal contact mappings, split mapped / needs-sync ──
        init_internal_contacts_needs_sync = rail.SetVariableOperator(
            task_id='init_internal_contacts_needs_sync',
            name='internal_contacts_needs_sync',
            value=[],
        )

        check_internal_contact_mapping_pre_filter = rail.SimpleHttpOperator(
            task_id='check_internal_contact_mapping_pre_filter',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=GetMapping"
                f"&name={quote(config.MAPPING_TYPE_NAMES['INTERNAL_CONTACT'])}"
                "&source={{ result('for_each_pre_filter_internal_contacts').get('_ae_employeeid_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        parse_check_internal_contact_mapping_pre_filter = rail.PythonOperator(
            task_id='parse_check_internal_contact_mapping_pre_filter',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('check_internal_contact_mapping_pre_filter'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['INTERNAL_CONTACT'],
            ),
        )

        # Already-mapped contact appended directly; unmapped contact queued for sync
        if_internal_contact_mapped = rail.IfOperator(
            task_id='if_internal_contact_mapped',
            test=lambda: bool(rail.result('parse_check_internal_contact_mapping_pre_filter')),
            yes_task='append_internal_contact_to_project_contact_list',
            no_task='record_internal_contact_needs_sync',
        )

        append_internal_contact_to_project_contact_list = rail.SetVariableOperator(
            task_id='append_internal_contact_to_project_contact_list',
            name='project_contact_list',
            append=True,
            value=lambda: {
                'd365_contact_guid': rail.result('for_each_pre_filter_internal_contacts').get('_ae_employeeid_value'),
                'pim_contact_id': rail.result('parse_check_internal_contact_mapping_pre_filter'),
            },
        )

        record_internal_contact_needs_sync = rail.SetVariableOperator(
            task_id='record_internal_contact_needs_sync',
            name='internal_contacts_needs_sync',
            append=True,
            value=lambda: rail.result('for_each_pre_filter_internal_contacts'),
        )

        for_each_pre_filter_internal_contacts = rail.ForEachOperator(
            task_id='for_each_pre_filter_internal_contacts',
            items='{{ result("parse_get_internal_contacts_for_project_from_d365") }}',
            start_task='check_internal_contact_mapping_pre_filter',
            end_task='for_each_pre_filter_internal_contacts_end',
        )

        for_each_pre_filter_internal_contacts_end = rail.EmptyOperator(
            task_id='for_each_pre_filter_internal_contacts_end',
        )

        # ── Trigger sync only for unmapped internal contacts ──────────────────
        get_internal_contacts_needs_sync = rail.GetVariableOperator(
            task_id='get_internal_contacts_needs_sync',
            name='internal_contacts_needs_sync',
        )

        parse_internal_contacts_needs_sync = rail.PythonOperator(
            task_id='parse_internal_contacts_needs_sync',
            python_callable=lambda: json.dumps((rail.result('get_internal_contacts_needs_sync') or {}).get('value', [])),
        )

        trigger_pre_sync_internal_contacts = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_pre_sync_internal_contacts',
            items='{{ result("parse_internal_contacts_needs_sync") }}',
            trigger_dag_id=config.internal_contact_dag_id,
            conf=lambda item: {
                'entity_guid': item.get('_ae_employeeid_value'),
                'triggered_by': rail.get_current_context()['dag_run'].run_id,
            },
        )

        wait_for_pre_sync_internal_contacts = rail.WaitForDagRunsSensor(
            task_id='wait_for_pre_sync_internal_contacts',
            dag_runs='{{ result("trigger_pre_sync_internal_contacts") }}',
        )

        # ── Phase 2: Append newly-synced internal contacts to project_contact_list ──
        check_new_internal_contact_mapping = rail.SimpleHttpOperator(
            task_id='check_new_internal_contact_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=GetMapping"
                f"&name={quote(config.MAPPING_TYPE_NAMES['INTERNAL_CONTACT'])}"
                "&source={{ result('for_each_new_internal_contacts').get('_ae_employeeid_value') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        parse_new_internal_contact_mapping = rail.PythonOperator(
            task_id='parse_new_internal_contact_mapping',
            python_callable=lambda: python_methods.extract_id_from_response_data(
                data=rail.result('check_new_internal_contact_mapping'),
                key='destinationId',
                type_name=config.MAPPING_TYPE_NAMES['INTERNAL_CONTACT'],
            ),
        )

        append_new_internal_contact_to_project_contact_list = rail.SetVariableOperator(
            task_id='append_new_internal_contact_to_project_contact_list',
            name='project_contact_list',
            append=True,
            value=lambda: {
                'd365_contact_guid': rail.result('for_each_new_internal_contacts').get('_ae_employeeid_value'),
                'pim_contact_id': rail.result('parse_new_internal_contact_mapping'),
            },
        )

        for_each_new_internal_contacts = rail.ForEachOperator(
            task_id='for_each_new_internal_contacts',
            items='{{ result("parse_internal_contacts_needs_sync") }}',
            start_task='check_new_internal_contact_mapping',
            end_task='for_each_new_internal_contacts_end',
        )

        for_each_new_internal_contacts_end = rail.EmptyOperator(
            task_id='for_each_new_internal_contacts_end',
        )

        # ── Read accumulated contact list and update PIM entity contacts ──────
        # Reads the full accumulated contact list (external + internal) from the dag-run variable store
        get_project_contact_list = rail.GetVariableOperator(
            task_id='get_project_contact_list',
            name='project_contact_list',
        )

        # Assembles the final deduplicated contact list payload for the UpdateEntityContacts API call
        build_update_entity_contacts_body = rail.PythonOperator(
            task_id='build_update_entity_contacts_body',
            python_callable=lambda: request_payload.build_update_entity_contacts_payload(
                config=config,
                pim_project_id=rail.result('parse_get_pim_project_id_from_project_mapper') or rail.result('parse_create_project_in_pim'),
                ae_contact_pim_id=rail.result('parse_get_pim_external_contact_id_from_mapper') or rail.result('parse_get_pim_external_contact_id_from_mapper_after_creation'),
                project_contact_list=(rail.result('get_project_contact_list') or {}).get('value'),
            ),
        )

        # Sends the complete deduplicated contact list to PIM; sets the primary ae_contact and all others as non-primary
        update_entity_contacts = rail.SimpleHttpOperator(
            task_id='update_entity_contacts',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{config.PIM_CUSTOM_API['ENTITY_CONTACTS']}"
                f"?function=UpdateEntityContacts"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_update_entity_contacts_body') }}",
        )

        # Writes an error entry to the PIM log if any upstream task in this run fails
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'entity_type': 'Project',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ result('parse_get_d365_project').get('msdyn_subject', '') if result('parse_get_d365_project') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
        )

        # Triggers the centralized error log child DAG to record this failure
        trigger_error_log = rail.TriggerDagRunOperator(
            task_id='trigger_error_log',
            trigger_dag_id=config.error_log_dag_id,
            conf={
                'entity_type': 'Project',
                'entity_guid': '{{ dag_run.conf.entity_guid }}',
                'entity_name': "{{ result('parse_get_d365_project').get('msdyn_subject', '') if result('parse_get_d365_project') else '' }}",
                'action': 'Sync',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
            },
            wait_for_completion=False,
        )


        # ── Task wiring ───────────────────────────────────────────────────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> get_d365_project >> parse_get_d365_project >> if_outline_level_is_equal_to_1 >> catch_and_log_errors

        if_outline_level_is_equal_to_1 >> rail.Label('No') >> stop_due_to_outline_level_not_1 >> catch_and_log_errors
        if_outline_level_is_equal_to_1 >> rail.Label('Yes') >> if_project_has_company

        if_project_has_company >> rail.Label('No') >> stop_due_to_no_company_associated
        if_project_has_company >> rail.Label('Yes') >> get_company_mapping >> parse_get_company_mapping >> if_company_mapping_not_found >> catch_and_log_errors

        if_company_mapping_not_found >> rail.Label('Yes') >> stop_due_to_no_company_mapping
        if_company_mapping_not_found >> rail.Label('No') >> get_pim_project_id_from_project_mapper >> parse_get_pim_project_id_from_project_mapper >> if_pim_project_id_exists_in_project_mapper >> catch_and_log_errors

        if_pim_project_id_exists_in_project_mapper >> rail.Label('Yes') >> build_update_project_in_pim_body >> update_project_in_pim >> if_division_present_in_get_d365_project
        if_pim_project_id_exists_in_project_mapper >> rail.Label('No') >> build_create_project_in_pim_body >> create_project_in_pim >> parse_create_project_in_pim >> build_add_project_mapping_body >> add_project_mapping >> if_division_present_in_get_d365_project

        if_division_present_in_get_d365_project >> rail.Label('Yes') >> process_division_udf >> wait_for_division_udf >> get_pim_division_id >> if_office_present_in_get_d365_project
        if_division_present_in_get_d365_project >> rail.Label('No') >> if_office_present_in_get_d365_project

        if_office_present_in_get_d365_project >> rail.Label('Yes') >> process_office_udf >> wait_for_office_udf >> get_pim_office_id >> if_segment_present_in_get_d365_project
        if_office_present_in_get_d365_project >> rail.Label('No') >> if_segment_present_in_get_d365_project

        if_segment_present_in_get_d365_project >> rail.Label('Yes') >> process_segment_udf >> wait_for_segment_udf >> get_pim_group_id >> build_update_project_udf_body
        if_segment_present_in_get_d365_project >> rail.Label('No') >> build_update_project_udf_body
        build_update_project_udf_body >> update_project_udf

        update_project_udf >> if_msdyn_customer_truthy_in_get_d365_project

        if_msdyn_customer_truthy_in_get_d365_project >> rail.Label('No') >> if_ae_contact_value_truthy_in_get_d365_project
        if_msdyn_customer_truthy_in_get_d365_project >> rail.Label('Yes') >> get_pim_external_organization_id_from_mapper >> parse_get_pim_external_organization_id_from_mapper >> if_pim_external_organization_id_exists_in_mapper

        if_pim_external_organization_id_exists_in_mapper >> rail.Label('Yes') >> get_existing_organizations_for_the_project >> if_external_organization_associated_with_the_project_with_specific_role
        if_pim_external_organization_id_exists_in_mapper >> rail.Label('No') >> process_external_organization

        if_external_organization_associated_with_the_project_with_specific_role >> rail.Label('Yes') >> if_ae_contact_value_truthy_in_get_d365_project
        if_external_organization_associated_with_the_project_with_specific_role >> rail.Label('No') >> process_external_organization

        process_external_organization >> wait_for_external_organization >> get_pim_external_organization_id_from_mapper_after_creation >> parse_get_pim_external_organization_id_from_mapper_after_creation >> build_link_external_organization_to_the_pim_project_body >> link_external_organization_to_the_pim_project >> if_ae_contact_value_truthy_in_get_d365_project

        if_ae_contact_value_truthy_in_get_d365_project >> rail.Label('Yes') >> get_pim_external_contact_id_from_mapper >> parse_get_pim_external_contact_id_from_mapper >> if_external_contact_for_ae_contact_exist_in_mapper
        if_ae_contact_value_truthy_in_get_d365_project >> rail.Label('No') >> get_external_contacts_for_project_from_d365

        if_external_contact_for_ae_contact_exist_in_mapper >> rail.Label('Yes') >> get_external_contacts_for_project_from_d365
        if_external_contact_for_ae_contact_exist_in_mapper >> rail.Label('No') >> process_external_contact_for_ae_contact_in_d365 >> wait_for_external_contact_for_ae_contact >> get_pim_external_contact_id_from_mapper_after_creation >> parse_get_pim_external_contact_id_from_mapper_after_creation >> get_external_contacts_for_project_from_d365

        get_external_contacts_for_project_from_d365 >> parse_get_external_contacts_for_project_from_d365 >> init_project_contact_list >> init_external_contacts_needs_sync >> for_each_pre_filter_external_contacts

        for_each_pre_filter_external_contacts >> check_contact_mapping_pre_filter >> parse_check_contact_mapping_pre_filter >> if_external_contact_mapped
        if_external_contact_mapped >> rail.Label('Yes') >> append_external_contact_to_project_contact_list >> for_each_pre_filter_external_contacts_end
        if_external_contact_mapped >> rail.Label('No') >> record_external_contact_needs_sync >> for_each_pre_filter_external_contacts_end
        for_each_pre_filter_external_contacts >> for_each_pre_filter_external_contacts_end

        for_each_pre_filter_external_contacts_end >> get_external_contacts_needs_sync >> parse_external_contacts_needs_sync >> trigger_pre_sync_external_contacts >> wait_for_pre_sync_external_contacts >> for_each_new_external_contacts

        for_each_new_external_contacts >> check_new_contact_mapping >> parse_new_contact_mapping >> append_new_external_contact_to_project_contact_list >> for_each_new_external_contacts_end
        for_each_new_external_contacts >> for_each_new_external_contacts_end

        for_each_new_external_contacts_end >> get_internal_contacts_for_project_from_d365 >> parse_get_internal_contacts_for_project_from_d365 >> init_internal_contacts_needs_sync >> for_each_pre_filter_internal_contacts

        for_each_pre_filter_internal_contacts >> check_internal_contact_mapping_pre_filter >> parse_check_internal_contact_mapping_pre_filter >> if_internal_contact_mapped
        if_internal_contact_mapped >> rail.Label('Yes') >> append_internal_contact_to_project_contact_list >> for_each_pre_filter_internal_contacts_end
        if_internal_contact_mapped >> rail.Label('No') >> record_internal_contact_needs_sync >> for_each_pre_filter_internal_contacts_end
        for_each_pre_filter_internal_contacts >> for_each_pre_filter_internal_contacts_end

        for_each_pre_filter_internal_contacts_end >> get_internal_contacts_needs_sync >> parse_internal_contacts_needs_sync >> trigger_pre_sync_internal_contacts >> wait_for_pre_sync_internal_contacts >> for_each_new_internal_contacts

        for_each_new_internal_contacts >> check_new_internal_contact_mapping >> parse_new_internal_contact_mapping >> append_new_internal_contact_to_project_contact_list >> for_each_new_internal_contacts_end
        for_each_new_internal_contacts >> for_each_new_internal_contacts_end

        for_each_new_internal_contacts_end >> get_project_contact_list >> build_update_entity_contacts_body >> update_entity_contacts >> catch_and_log_errors

        catch_and_log_errors >> trigger_error_log

        return dag


rail.for_each_instance(create_dag)
