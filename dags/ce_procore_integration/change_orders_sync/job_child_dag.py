import rail
from datetime import timedelta
from ce_procore_integration.util_dags.utils import normalize_ce_identifier
from ce_procore_integration.change_orders_sync.utils.util import (
    build_cop_origin_id,
    parse_budget_line_items,
    create_revenue_wbs_code_definition,
    aggregate_cost_budgets_by_flat_code,
    aggregate_contract_amounts_by_phase_category
)
from ce_procore_integration.change_orders_sync.utils.constants import (
    cost_type_name,
    cost_type_type,
    cost_code_segment_name,
    cost_code_segment_type
)


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.job_child_dag_id,
        description='ComputerEase to Procore Change Order Sync - Job Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_prime_contract',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        fetch_prime_contract = rail.ProcoreApiOperator(
            task_id='fetch_prime_contract',
            endpoint='/prime_contracts',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda response: {
                'all': response or [],
                'prime_contract': min(
                    (c for c in (response or []) if c.get('created_at')),
                    key=lambda c: c['created_at'],
                    default={}
                )
            }
        )

        check_approved_prime_contract_exists = rail.IfOperator(
            task_id='check_approved_prime_contract_exists',
            test=lambda: bool(
                ((rail.result('fetch_prime_contract') or {}).get('prime_contract') or {}).get('id') \
                and rail.result('fetch_prime_contract')['prime_contract']['status'].upper() == 'APPROVED'
            ),
            yes_task='fetch_wbs_codes',
            no_task='log_invalid_prime_contract'
        )

        log_invalid_prime_contract = rail.WriteLogOperator(
            task_id='log_invalid_prime_contract',
            message='Prime Contract not found in Procore',
            severity='Error/Exception',
            properties={
                'job_code': '{{ dag_run.conf.batch.job_code }}',
                'job_name': '{{ dag_run.conf.batch.job_name }}',
                'error_message': "Change Orders not synced - First created Prime Contract was not \
                    {{ 'approved' if result('fetch_prime_contract').get('prime_contract') else 'found' }}"
            }
        )


        def identify_missing_wbs_codes(response, dag_run):
            # Identify missing WBS codes: regular (cost_budget > 0) and revenue (contract_amount > 0)
            existing_wbs_codes = {
                normalize_ce_identifier(item['flat_code']): item['id']
                for item in response
                if item.get('flat_code')
            }
            batch = dag_run.conf['batch']
            wbs_codes_to_check = batch.get('wbs_codes_to_check', [])
            rfcs = batch['rfcs']

            missing_wbs_codes = []

            cost_budget_amounts = aggregate_cost_budgets_by_flat_code(rfcs)

            for wbs_item in wbs_codes_to_check:
                flat_code = wbs_item.get('flat_code')
                if flat_code and cost_budget_amounts.get(flat_code, 0) > 0 and flat_code not in existing_wbs_codes:
                    missing_wbs_codes.append(wbs_item)

            revenue_aggregation = aggregate_contract_amounts_by_phase_category(rfcs)

            for phase_category, rev_data in revenue_aggregation.items():
                if rev_data['total_amount'] <= 0:
                    continue

                revenue_flat_code = f"{phase_category}.{config.revenue_cost_type}"

                if revenue_flat_code not in existing_wbs_codes:
                    revenue_wbs_code = create_revenue_wbs_code_definition(
                        revenue_flat_code,
                        rev_data['phase_code'],
                        rev_data['category_code'],
                        config.revenue_cost_type
                    )
                    missing_wbs_codes.append(revenue_wbs_code)
            cost_code_segment_id = next(
                (
                    item['segment']['id']
                    for wbs in response
                    for item in wbs.get('segment_items', [])
                    if item.get('segment', {}).get('name') == cost_code_segment_name
                    and item.get('segment', {}).get('type') == cost_code_segment_type
                ),
                None
            )
            cost_type_segment_id = next(
                (
                    item['segment']['id']
                    for wbs in response
                    for item in wbs.get('segment_items', [])
                    if item.get('segment', {}).get('name') == cost_type_name
                    and item.get('segment', {}).get('type') == cost_type_type
                ),
                None
            )
            return {
                'missing': missing_wbs_codes,
                'existing': existing_wbs_codes,
                'cost_code_segment_id': cost_code_segment_id,
                'cost_type_segment_id': cost_type_segment_id
            }
        fetch_wbs_codes = rail.ProcoreApiOperator(
            task_id='fetch_wbs_codes',
            endpoint="/projects/{{ dag_run.conf.project_id }}/work_breakdown_structure/wbs_codes",
            method='GET',
            data_handler=lambda response, dag_run: identify_missing_wbs_codes(response, dag_run)
        )

        has_missing_wbs_codes = rail.IfOperator(
            task_id='has_missing_wbs_codes',
            test="{{ result('fetch_wbs_codes').missing | length > 0 }}",
            yes_task='trigger_wbs_creator',
            no_task='validate_wbs_code_creation'
        )

        trigger_wbs_creator = rail.TriggerDagRunOperator(
            task_id='trigger_wbs_creator',
            trigger_dag_id=config.wbs_code_creator_dag_id,
            conf=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'wbs_codes_to_create': rail.result('fetch_wbs_codes')['missing'],
                'cost_code_segment_id': rail.result('fetch_wbs_codes')['cost_code_segment_id'],
                'cost_type_segment_id': rail.result('fetch_wbs_codes')['cost_type_segment_id']
            }
        )

        wait_for_wbs_creator = rail.WaitForDagRunsSensor(
            task_id='wait_for_wbs_creator',
            dag_runs="{{ result('trigger_wbs_creator') }}",
            execution_timeout=timedelta(minutes=30)
        )

        gather_wbs_creator = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_wbs_creator',
            dag_runs='{{ result("trigger_wbs_creator") }}',
            dagrun_task_id='compile_results'
        )

        def get_created_wbs_results():
            """Returns the list of {flat_code: id_or_error}"""
            try:
                results = rail.result('gather_wbs_creator')
            except Exception:  # pylint: disable=broad-except
                results = None
            return [r for r in (results or []) if r and isinstance(r, dict)]

        def build_wbs_code_payload(dag_run):
            wbs_code_mapping = dict(rail.result('fetch_wbs_codes')['existing'])

            failed_codes = set()
            failed_reasons = {}
            
            for result in get_created_wbs_results():
                for flat_code, value in result.items():
                    if not flat_code:
                        continue
                    if isinstance(value, str):
                        failed_codes.add(flat_code)
                        failed_reasons[flat_code] = value
                    else:
                        wbs_code_mapping[flat_code] = value
            
            rfcs = dag_run.conf['batch']['rfcs']
            syncable = []
            skipped = []
            for rfc in rfcs:
                rfc_flat_codes = set()
                rfc_line_items = parse_budget_line_items(
                    rfc.get('budget_line_items', '[]')
                )
                for item in rfc_line_items:
                    flat_code = item.get('flat_code')
                    if flat_code:
                        rfc_flat_codes.add(flat_code)

                blocking = sorted(rfc_flat_codes & failed_codes)
                if blocking:
                    skipped.append({'rfc': rfc, 'failed_codes': blocking})
                else:
                    syncable.append(rfc)
            return {
                'wbs_codes': wbs_code_mapping,
                'syncable': syncable,
                'skipped': skipped,
                'failed_reasons': failed_reasons
            }

        validate_wbs_code_creation = rail.PythonOperator(
            task_id='validate_wbs_code_creation',
            python_callable=build_wbs_code_payload
        )

        has_invalid_rfcs = rail.IfOperator(
            task_id='has_invalid_rfcs',
            test='{{ result("validate_wbs_code_creation").skipped | length > 0 }}',
            yes_task='write_invalid_rfc_exception',
            no_task='has_valid_rfcs'
        )

        write_invalid_rfc_exception = rail.WriteLogOperator(
            task_id='write_invalid_rfc_exception',
            message='RFC Skipped - WBS Code Creation Failed',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda dag_run: [
                {
                    'code': entry['rfc']['co_number'],
                    'job_code': entry['rfc']['job_code'],
                    'company_id': dag_run.conf['company_id'],
                    'status': 'Skipped',
                    'reason': 'RFC not synced; WBS codes could not be created: '
                        + ', '.join([
                            rail.result('validate_wbs_code_creation')['failed_reasons'][code]
                            for code in entry['failed_codes']
                        ])
                } for entry in rail.result('validate_wbs_code_creation')['skipped']
            ]
        )

        has_valid_rfcs = rail.IfOperator(
            task_id='has_valid_rfcs',
            test="{{ result('validate_wbs_code_creation').syncable | length > 0 }}",
            yes_task='fetch_change_order_packages',
            no_task='catch_error'
        )

        fetch_change_order_packages = rail.ProcoreApiOperator(
            task_id='fetch_change_order_packages',
            endpoint='/change_order_packages',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}',
                'filters[contract_id]': '{{ result("fetch_prime_contract").prime_contract.id }}',
            },
            data_handler=lambda response: {
                cop['origin_id']: cop['id'] for cop in response if cop.get('origin_id')
            }
        )

        fetch_potential_change_orders = rail.ProcoreApiOperator(
            task_id='fetch_potential_change_orders',
            endpoint='/potential_change_orders',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}',
                'filters[contract_id]': '{{ result("fetch_prime_contract").prime_contract.id }}'
            },
            data_handler=lambda response: {
                cop['title'].split()[1].removeprefix('#'): cop['id']
                for cop in response
                if cop['title'].startswith('CE #')
            }
        )

        fetch_budget_changes = rail.ProcoreApiOperator(
            task_id='fetch_budget_changes',
            endpoint='/projects/{{ dag_run.conf.project_id }}/budget_changes',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data_handler=lambda res: {
                bc['title']: str(bc['id']) for bc in res
            } if res else {}
        )

        def get_budget_change_line_items(response):
            if not response or len(response) == 0:
                return {}
            required_budget_changes = rail.result('fetch_budget_changes').values()
            budget_change_line_items = {}
            for bc in response:
                bc_id = str(bc['budget_change_id'])
                if bc_id in required_budget_changes:
                    budget_change_line_items.setdefault(bc_id, []).append(bc)
            return budget_change_line_items

        fetch_budget_change_line_items = rail.ProcoreApiOperator(
            task_id='fetch_budget_change_line_items',
            endpoint='/companies/{{ dag_run.conf.company_id }}/projects/{{ dag_run.conf.project_id }}/budget_changes/adjustment_line_items',
            method='GET',
            version='2.0',
            data_handler=get_budget_change_line_items
        )

        trigger_each_rfc_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_each_rfc_child',
            trigger_dag_id=config.rfc_child_dag_id,
            items=lambda dag_run: dag_run.conf['batch']['rfcs'],
            conf=lambda item, dag_run: {
                **dict(item.items()),
                **{
                    'job_code': item['job_code'],
                    'job_name': item['job_name'],
                    'project_id': dag_run.conf['project_id'],
                    'prime_contract_id': rail.result('fetch_prime_contract')['prime_contract']['id'],
                    'cop_id': rail.result('fetch_change_order_packages').get(build_cop_origin_id(item['job_code'], item['co_number'])),
                    'pco_id': rail.result('fetch_potential_change_orders').get(item['co_number']),
                    'budget_change_id': rail.result('fetch_budget_changes').get(item['co_number']),
                    'budget_change_line_items': rail.result('fetch_budget_change_line_items').get(rail.result('fetch_budget_changes').get(item['co_number'])),
                    'wbs_code_mapping': rail.result('validate_wbs_code_creation')['wbs_codes']
                }
            }
        )

        wait_for_each_rfc_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_each_rfc_child',
            dag_runs="{{ result('trigger_each_rfc_child') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties={
                'job_code': "{{ dag_run.conf.batch.job_code }}",
                'job_name': "{{ dag_run.conf.batch.job_name }}",
                'error_message': "Change Order sync failed - {{ get_error_message() }}"
            }
        )

        batch_task >> fetch_prime_contract >> check_approved_prime_contract_exists

        check_approved_prime_contract_exists >> rail.Label(
            'Yes') >> fetch_wbs_codes >> has_missing_wbs_codes
        check_approved_prime_contract_exists >> rail.Label(
            'No') >> log_invalid_prime_contract >> catch_error

        has_missing_wbs_codes >> rail.Label('No') >> validate_wbs_code_creation
        has_missing_wbs_codes >> rail.Label('Yes') >> trigger_wbs_creator >> wait_for_wbs_creator 
        wait_for_wbs_creator >> gather_wbs_creator >> validate_wbs_code_creation >> has_invalid_rfcs

        has_invalid_rfcs >> rail.Label('No') >> has_valid_rfcs
        has_invalid_rfcs >> rail.Label('Yes') >> write_invalid_rfc_exception >> has_valid_rfcs

        has_valid_rfcs >> rail.Label('No') >> catch_error
        has_valid_rfcs >> rail.Label('Yes') >> fetch_change_order_packages >> fetch_potential_change_orders
        fetch_potential_change_orders >> fetch_budget_changes >> fetch_budget_change_line_items
        fetch_budget_change_line_items >> trigger_each_rfc_child >> wait_for_each_rfc_child >> catch_error

        batch_task >> catch_error

    return dag


rail.for_each_instance(create_dag_instance)
