from datetime import timedelta
import rail
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):  # pylint: disable= too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.wbs_code_creator_dag_id,
        description='Utility DAG to create missing WBS codes in Procore',
        integration_type='generic',
        company_key=config.instance,
        is_paused_upon_creation=config.is_paused_upon_creation,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'execution_timeout': timedelta(minutes=30),
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='should_fetch_segments',
            end_task='catch_error',
            execution_timeout=timedelta(minutes=30)
        )

        should_fetch_segments = rail.IfOperator(
            task_id='should_fetch_segments',
            test=lambda dag_run: dag_run.conf.get('cost_code_segment_id') is None or dag_run.conf.get('cost_type_segment_id') is None,
            yes_task='fetch_segments',
            no_task='should_fetch_cost_codes'
        )

        fetch_segments = rail.ProcoreApiOperator(
            task_id='fetch_segments',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["project_id"]}/work_breakdown_structure/segments',
            method='GET',
            data_handler=lambda segments: {
                'cost_code_segment_id': next((s['id'] for s in segments if s['name'] == 'Cost Code'), None),
                'cost_type_segment_id': next((s['id'] for s in segments if s['name'] == 'Cost Type'), None)
            }
        )

        should_fetch_cost_codes = rail.IfOperator(
            task_id='should_fetch_cost_codes',
            test=lambda dag_run: dag_run.conf.get('cost_codes') is None,
            yes_task='fetch_cost_code_items',
            no_task='should_fetch_cost_types'
        )

        fetch_cost_code_items = rail.ProcoreApiOperator(
            task_id='fetch_cost_code_items',
            endpoint='/cost_codes',
            method='GET',
            query_params={
                'view': 'erp_compact',
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda items: {
                normalize_ce_identifier(item['full_code']): item['id'] for item in items
            }
        )

        should_fetch_cost_types = rail.IfOperator(
            task_id='should_fetch_cost_types',
            test=lambda dag_run: dag_run.conf.get('cost_types') is None,
            yes_task='fetch_cost_type_items',
            no_task='validate_payloads'
        )

        fetch_cost_type_items = rail.ProcoreApiOperator(
            task_id='fetch_cost_type_items',
            endpoint='/line_item_types',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda items: {
                item['code']: item['id'] for item in items}
        )

        def validate_and_prepare_wbs_payloads(dag_run):
            wbs_codes_to_create = dag_run.conf['wbs_codes_to_create']
            cost_code_segment_id = dag_run.conf.get('cost_code_segment_id') or rail.result('fetch_segments')['cost_code_segment_id']
            cost_type_segment_id = dag_run.conf.get('cost_type_segment_id') or rail.result('fetch_segments')['cost_type_segment_id']
            cost_code_items = dag_run.conf.get('cost_codes') or rail.result('fetch_cost_code_items')
            # Uppercase keys regardless of source (conf-supplied vs fetched) so CE-derived
            # phase/category probes match; the fetch branch is already uppercased, this is idempotent.
            cost_code_items = {
                normalize_ce_identifier(k): v for k, v in (cost_code_items or {}).items()
            }
            cost_type_items = dag_run.conf.get('cost_types') or rail.result('fetch_cost_type_items')

            # Remove duplicates based on flat_code
            unique_wbs_codes = {}
            for wbs_item in wbs_codes_to_create:
                flat_code = wbs_item.get('flat_code', '')
                if flat_code and flat_code not in unique_wbs_codes:
                    unique_wbs_codes[flat_code] = wbs_item

            wbs_payloads = []
            errors = {}

            for wbs_item in unique_wbs_codes.values():
                phase_code = wbs_item.get('phase_code', '').strip()
                category_code = wbs_item.get('category_code', '').strip()
                cost_type = wbs_item.get('cost_type', '').strip()
                flat_code = wbs_item.get('flat_code', '')

                # Build segment_items array
                segment_items = []

                # Check Phase (if exists)
                if phase_code:
                    phase_item_id = cost_code_items.get(phase_code)
                    if not phase_item_id:
                        errors[flat_code] = f"Cannot create WBS code {flat_code} - Phase '{phase_code}' not found in Procore"
                        continue
                    segment_items.append({
                        "segment_id": cost_code_segment_id,
                        "segment_item_id": phase_item_id
                    })

                # Check Category
                # If phase exists, category path is phase-category, otherwise just category
                if phase_code and category_code:
                    category_path = f"{phase_code}-{category_code}"
                elif category_code:
                    category_path = category_code
                else:
                    category_path = None

                if category_path:
                    category_item_id = cost_code_items.get(category_path)
                    if not category_item_id:
                        errors[flat_code] = f"Cannot create WBS code {flat_code} - Category '{category_path}' not found in Procore"
                        continue
                    segment_items.append({
                        "segment_id": cost_code_segment_id,
                        "segment_item_id": category_item_id
                    })

                # Check Cost Type
                if cost_type:
                    cost_type_item_id = cost_type_items.get(cost_type)
                    if not cost_type_item_id:
                        errors[flat_code] = f"Cannot create WBS code {flat_code} - Cost Type '{cost_type}' not found in Procore"
                        continue
                    segment_items.append({
                        "segment_id": cost_type_segment_id,
                        "segment_item_id": cost_type_item_id
                    })

                # Add to payloads
                wbs_payloads.append({
                    "segment_items": segment_items
                })

            return {
                'wbs_payloads': wbs_payloads,
                'errors': errors
            }

        validate_payloads = rail.PythonOperator(
            task_id='validate_payloads',
            python_callable=validate_and_prepare_wbs_payloads
        )

        if_wbs_payloads_exist = rail.IfOperator(
            task_id='if_wbs_payloads_exist',
            test=lambda: len(rail.result("validate_payloads")
                             ['wbs_payloads']) > 0,
            yes_task='create_wbs_codes',
            no_task='compile_results'
        )

        create_wbs_codes = rail.ProcoreApiOperator(
            task_id='create_wbs_codes',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["project_id"]}/work_breakdown_structure/wbs_codes/bulk_create',
            method='PATCH',
            data=lambda: {
                "bulk": rail.result('validate_payloads')['wbs_payloads']
            },
            data_handler=lambda response: response if response else []
        )

        def compile_final_results(dag_run):
            # Get all unique flat codes from input
            wbs_codes_to_create = dag_run.conf['wbs_codes_to_create']
            unique_flat_codes = set()
            for wbs_item in wbs_codes_to_create:
                flat_code = wbs_item.get('flat_code', '')
                if flat_code:
                    unique_flat_codes.add(normalize_ce_identifier(flat_code))

            # Get validation errors first
            validation_result = rail.result('validate_payloads')
            validation_errors = validation_result.get('errors', {})

            # Get created WBS codes response
            created_wbs_response = rail.result('create_wbs_codes', {})

            # Initialize results for all unique flat codes
            final_results = {}

            # First, handle validation errors (these never made it to API call)
            for flat_code, error_msg in validation_errors.items():
                final_results[normalize_ce_identifier(flat_code)] = error_msg

            # Then, handle API response - check which ones were successfully created
            entities = created_wbs_response.get('entities', []) if isinstance(
                created_wbs_response, dict) else []
            created_flat_codes = set()

            for entity in entities:
                flat_code = normalize_ce_identifier(entity.get('flat_code', ''))
                if flat_code and entity.get('id'):
                    final_results[flat_code] = entity['id']
                    created_flat_codes.add(flat_code)

            # Any flat_code that doesn't have validation errors or successful creation means it failed at API level
            for flat_code in unique_flat_codes:
                if flat_code not in final_results:
                    final_results[flat_code] = f"WBS code creation failed for {flat_code}"

            return final_results

        compile_results = rail.PythonOperator(
            task_id='compile_results',
            python_callable=compile_final_results
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> catch_error
        batch_task >> should_fetch_segments

        should_fetch_segments >> rail.Label('Yes') >> fetch_segments >> should_fetch_cost_codes
        should_fetch_segments >> rail.Label('No') >> should_fetch_cost_codes

        should_fetch_cost_codes >> rail.Label('Yes') >> fetch_cost_code_items >> should_fetch_cost_types
        should_fetch_cost_codes >> rail.Label('No') >> should_fetch_cost_types

        should_fetch_cost_types >> rail.Label('Yes') >> fetch_cost_type_items >> validate_payloads
        should_fetch_cost_types >> rail.Label('No') >> validate_payloads >> if_wbs_payloads_exist

        if_wbs_payloads_exist >> rail.Label('Yes') >> create_wbs_codes >> compile_results
        if_wbs_payloads_exist >> rail.Label('No') >> compile_results >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
