"""
Process UDFs DAG — Generic ref-data resolve-or-create for Division / Office / Group.

Triggered by other sync DAGs (lead_sync, project_sync, etc.) via
TriggerDagRunOperator with dag_run.conf containing:

  - mapping_type_name : ExternalIntegrationMapping type name
                        e.g. 'D365 Market to PIM Division'
  - pim_add_function  : DropdownValues.ashx function name
                        e.g. 'AddDivision', 'AddOffice', 'AddGroup'
  - source_guid       : D365 GUID of the source entity
  - name              : display name to create in PIM when mapping is absent

Flow
----
get_udf_mapping >> parse_get_udf_mapping
  -> check_mapping_exists
      Yes: get_destination_id   (from existing mapping)
      No:  build_create_udf_body >> create_udf >> parse_create_udf
           >> build_add_udf_mapping_body >> add_udf_mapping >> get_destination_id
"""
import json
from datetime import timedelta
import rail
from airflow.models import Variable
from associatedengineeringgroup.deltek_pim_d365_integration.config import (
    PIM_CUSTOM_API,
)
from associatedengineeringgroup.deltek_pim_d365_integration.utils.python_methods import (
    extract_id_from_response_data,
    safe_json_response,
)


def create_dag(config):
    """Create the generic UDF processing DAG for a given instance."""

    with rail.create_airflow_dag(
        dag_id=config.process_udfs_dag_id,
        description='Generic UDF resolve-or-create for Division/Office/Group',
        integration_type='generic',
        company_key=config.company_key,
        schedule_interval=None,
        max_active_runs=1,
        tags=['pim_d365', 'udf', 'sync'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        # View dag_run.conf
        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_conf',
        )

        # ── Batch task gate ───────────────────────────────────────────────────
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'{config.process_udfs_dag_id}_can_run_batch_task',
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='get_udf_mapping',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_udf_mapping',
            end_task='get_destination_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ── 1. Check if mapping already exists ───────────────────────────────
        # Looks up ExternalIntegrationMapping by source GUID to check if a PIM mapping exists
        get_udf_mapping = rail.SimpleHttpOperator(
            task_id='get_udf_mapping',
            method='GET',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=GetMapping"
                "&source={{ dag_run.conf.get('source_guid', '') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
            },
            response_filter=lambda response: response.json(),
        )

        # Extracts the destinationId from the mapping response; None if no mapping exists
        parse_get_udf_mapping = rail.PythonOperator(
            task_id='parse_get_udf_mapping',
            python_callable=lambda dag_run: extract_id_from_response_data(
                data=rail.result('get_udf_mapping'),
                key='destinationId',
                type_name=dag_run.conf.get('mapping_type_name'),
            ),
        )

        # Branches to the existing destination ID or triggers UDF creation
        check_mapping_exists = rail.IfOperator(
            task_id='check_mapping_exists',
            test=lambda: bool(rail.result('parse_get_udf_mapping')),
            yes_task='get_destination_id',
            no_task='build_create_udf_body',
        )

        # ── 2. Create the UDF in PIM DropdownValues ──────────────────────────
        # Builds the JSON payload for creating a new UDF dropdown entry
        build_create_udf_body = rail.PythonOperator(
            task_id='build_create_udf_body',
            python_callable=lambda: json.dumps({
                'name': rail.get_current_context()['dag_run'].conf.get('name', ''),
            }),
        )

        # Posts the new UDF entry to PIM DropdownValues via the configured add function
        create_udf = rail.SimpleHttpOperator(
            task_id='create_udf',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['DROPDOWN_VALUES']}"
                "?function={{ dag_run.conf.get('pim_add_function', '') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_create_udf_body') }}",
            response_filter=lambda response: response.json(),
        )

        # Extracts the newly created UDF entry ID from the create response
        parse_create_udf = rail.PythonOperator(
            task_id='parse_create_udf',
            python_callable=lambda: extract_id_from_response_data(
                data=rail.result('create_udf'),
                key='id',
            ),
        )

        # ── 3. Register the mapping ──────────────────────────────────────────
        # Builds the ExternalIntegrationMapping body linking the D365 GUID to the new PIM UDF ID
        build_add_udf_mapping_body = rail.PythonOperator(
            task_id='build_add_udf_mapping_body',
            python_callable=lambda: json.dumps({
                'sourceGuid': rail.get_current_context()['dag_run'].conf.get('source_guid'),
                'destinationId': rail.result('parse_create_udf'),
            }),
        )

        # Registers the new UDF mapping in ExternalIntegrationMapping so future runs find it
        add_udf_mapping = rail.SimpleHttpOperator(
            task_id='add_udf_mapping',
            method='POST',
            http_conn_id=config.pim_conn_id,
            endpoint=(
                f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
                f"?function=AddMapping"
                "&name={{ dag_run.conf.get('mapping_type_name', '') }}"
            ),
            headers={
                'Authorization': f"Bearer {{{{ var.value.{config.PIM_TOKEN_VAR_PREFIX}_{config.instance} }}}}",
                'Content-Type': 'application/json',
            },
            data="{{ result('build_add_udf_mapping_body') }}",
        )

        # ── 4. Resolve destinationId from whichever path ran ────────────────
        # Returns the resolved PIM UDF ID — from either the existing mapping or the newly created entry
        def _get_destination_id():
            return rail.result('parse_get_udf_mapping') or rail.result('parse_create_udf')

        get_destination_id = rail.PythonOperator(
            task_id='get_destination_id',
            python_callable=_get_destination_id,
        )

        # ── Task wiring ──────────────────────────────────────────────────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> get_destination_id
        can_run_batch_task >> rail.Label('No') >> get_udf_mapping >> parse_get_udf_mapping >> check_mapping_exists

        check_mapping_exists >> rail.Label('Yes') >> get_destination_id
        check_mapping_exists >> rail.Label('No') >> build_create_udf_body >> create_udf >> parse_create_udf >> build_add_udf_mapping_body >> add_udf_mapping >> get_destination_id

    return dag


rail.for_each_instance(create_dag)
