from datetime import timedelta
import rail
from procore_ce_integration.job_structure_sync.utils.constants import WBSType


def create_dag_instance(config):
    per_contract_dag_id = config.prime_contract_line_items_dag_id.replace(
        'prime_contract_line_items', 'per_contract_line_items'
    )

    with rail.create_airflow_dag(
        dag_id=config.prime_contract_line_items_dag_id,
        description='Prime Contract Line Items DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_prime_contracts',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_prime_contracts = rail.ProcoreApiOperator(
            task_id='get_prime_contracts',
            endpoint='/prime_contracts?project_id={{ dag_run.conf.project_id }}',
            method='GET',
            data_handler=lambda response: {
                'all': response or [],
                'approved': [
                    {'contract_id': c['id']}
                    for c in (response or [])
                    if c.get('status', '').upper() == 'APPROVED'
                ]
            }
        )

        if_has_approved_contracts = rail.IfOperator(
            task_id='if_has_approved_contracts',
            test=lambda: len(rail.result('get_prime_contracts')['approved']) > 0,
            yes_task='trigger_per_contract_fetch',
            no_task='catch_error'
        )

        trigger_per_contract_fetch = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_per_contract_fetch',
            trigger_dag_id=per_contract_dag_id,
            items=lambda: rail.result('get_prime_contracts')['approved'],
            conf=lambda item, dag_run: {
                'contract_id': item['contract_id'],
                'project_id': dag_run.conf['project_id']
            }
        )

        wait_for_per_contract = rail.WaitForDagRunsSensor(
            task_id='wait_for_per_contract',
            dag_runs='{{ result("trigger_per_contract_fetch") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_per_contract_line_items = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_per_contract_line_items',
            dag_runs='{{ result("trigger_per_contract_fetch") }}',
            dagrun_task_id='get_line_items'
        )

        def parse_all_line_items(dag_run):
            wbs_type = dag_run.conf.get('wbs_type')
            ce_contract_by_category = dag_run.conf.get('contract_by_category')

            gathered = rail.result('gather_per_contract_line_items') or []
            all_line_items = []
            for items in gathered:
                if items:
                    all_line_items.extend(items)

            contracts = {}
            has_at_phase_level = False
            has_at_category_level = False

            for line_item in all_line_items:
                amount = float(line_item.get('amount', 0))
                if amount == 0:
                    continue
                if not line_item.get('cost_code') or not line_item['cost_code'].get('id'):
                    continue

                cost_code_id = str(line_item['cost_code']['id'])
                parent_id = (line_item.get('cost_code') or {}).get('parent', {}).get('id')

                if parent_id is not None:
                    has_at_category_level = True
                elif wbs_type == WBSType.JOB_CAT:
                    has_at_category_level = True
                else:
                    has_at_phase_level = True

                contracts[cost_code_id] = contracts.get(cost_code_id, 0) + amount

            if has_at_phase_level and has_at_category_level:
                raise Exception(
                    "Mixed phase and category level line items detected across Prime Contracts. "
                    "Ensure all contract line items are at the same level."
                )

            if wbs_type == WBSType.JOB_CAT:
                inferred_contract_by_category = True # Can only be True, if WBS type is JOB_CAT
            elif has_at_category_level:
                inferred_contract_by_category = True
            elif has_at_phase_level:
                inferred_contract_by_category = False
            else:
                inferred_contract_by_category = None  # No non-zero line items found

            if inferred_contract_by_category is not None and ce_contract_by_category is not None:
                if inferred_contract_by_category != ce_contract_by_category:
                    detected_name = 'category' if inferred_contract_by_category else 'phase'
                    stored_name = 'category' if ce_contract_by_category else 'phase'
                    raise Exception(
                        f"Contract level mismatch: Prime Contract line items are at {detected_name} level "
                        f"but CE job is configured for {stored_name} level."
                    )

            return {
                'contracts': contracts,
                'cost_code_ids': list(contracts.keys()),
                'contract_by_category': inferred_contract_by_category if inferred_contract_by_category is not None else ce_contract_by_category
            }

        parse_line_items = rail.PythonOperator(
            task_id='parse_line_items',
            python_callable=parse_all_line_items
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PRIME_CONTRACT',
                'entity_code': dag_run.conf['project_number'],
                'procore_project_id': dag_run.conf['project_id'],
                'procore_project_name': dag_run.conf.get('project_name', ''),
                'error_message': 'Contract Amounts not Synced: ' + rail.render_template('{{ get_error_message() }}')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_prime_contracts >> if_has_approved_contracts
        if_has_approved_contracts >> rail.Label('Yes') >> trigger_per_contract_fetch >> wait_for_per_contract >> gather_per_contract_line_items >> parse_line_items >> catch_error
        if_has_approved_contracts >> rail.Label('No') >> catch_error
        batch_task >> catch_error
        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
