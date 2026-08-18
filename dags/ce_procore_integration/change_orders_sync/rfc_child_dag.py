import rail
from datetime import timedelta
from ce_procore_integration.change_orders_sync.utils.util import (
    is_non_zero_value,
    build_cop_origin_id,
    build_pco_origin_id,
    parse_budget_line_items,
    build_phase_category_code,
    create_adjustment_line_item
)


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.rfc_child_dag_id,
        description='Updates Contract amount as Revenue in potential change order SOVs and updates budget amount',
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
            start_task='is_co_package_exists',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        is_co_package_exists = rail.IfOperator(
            task_id='is_co_package_exists',
            test="{{ dag_run.conf.cop_id | is_truthy }}",
            yes_task='update_co_package' if config.should_allow_update else 'is_pco_exists',
            no_task='create_co_package'
        )

        def build_co_package_payload(dag_run):
            co_number = dag_run.conf['co_number']

            budget_line_items = parse_budget_line_items(
                dag_run.conf.get('budget_line_items', '[]'))

            grand_total = sum(float(item.get('contract_amount', 0) or 0)
                              for item in budget_line_items)

            payload = {
                "change_order": {
                    "title": f"CE #{co_number} - {dag_run.conf['job_name']}",
                    "description": dag_run.conf.get('description', ''),
                    "grand_total": str(grand_total),
                    "executed": False
                },
                "project_id": dag_run.conf['project_id'],
                "contract_id": dag_run.conf['prime_contract_id']
            }

            due_date = dag_run.conf.get('date')
            if due_date:
                if 'T' not in due_date:
                    due_date = f"{due_date}T00:00:00Z"
                payload["change_order"]["due_date"] = due_date

            return payload

        create_co_package = rail.ProcoreApiOperator(
            task_id='create_co_package',
            endpoint='/change_order_packages',
            method='POST',
            data=build_co_package_payload
        )

        if config.should_allow_update:
            update_co_package = rail.ProcoreApiOperator(
                task_id='update_co_package',
                endpoint="/change_order_packages/{{ dag_run.conf.cop_id or result('create_co_package').get('id') }}",
                method='PATCH',
                data=build_co_package_payload
            )

        is_pco_exists = rail.IfOperator(
            task_id='is_pco_exists',
            test="{{ dag_run.conf.pco_id | is_truthy }}",
            yes_task='update_potential_co' if config.should_allow_update else 'find_pcco_line_items',
            no_task='create_potential_co'
        )

        def build_potential_co_payload(dag_run):
            co_number = dag_run.conf['co_number']
            job_code = dag_run.conf['job_code']

            payload = {
                "change_order": {
                    "title": f"CE #{co_number} - {dag_run.conf['job_name']}", # Task:fetch_potential_change_orders uses this title for pco identification 
                    "description": dag_run.conf.get('description', ''),
                    "origin_id": build_pco_origin_id(job_code, co_number),
                    "change_reason": config.change_reason
                },
                "project_id": dag_run.conf['project_id'],
                "contract_id": dag_run.conf['prime_contract_id']
            }

            due_date = dag_run.conf.get('date')
            if due_date:
                if 'T' not in due_date:
                    due_date = f"{due_date}T00:00:00Z"
                payload["change_order"]["due_date"] = due_date

            return payload

        create_potential_co = rail.ProcoreApiOperator(
            task_id='create_potential_co',
            endpoint='/potential_change_orders',
            method='POST',
            data=build_potential_co_payload
        )

        if config.should_allow_update:
            update_potential_co = rail.ProcoreApiOperator(
                task_id='update_potential_co',
                endpoint='/potential_change_orders/{{ dag_run.conf.pco_id }}',
                method='PATCH',
                data=build_potential_co_payload
            )

        find_pcco_line_items = rail.ProcoreApiOperator(
            task_id='find_pcco_line_items',
            endpoint="/potential_change_orders/{{ dag_run.conf.pco_id or result('create_potential_co').get('id') }}/line_items",
            method='GET',
            query_params={
                'project_id': "{{ dag_run.conf.project_id }}"
            }
        )

        def build_potential_co_line_items(dag_run):
            # Build PCO line items aggregated by phase-category from contract amounts
            potential_co_id = dag_run.conf.get('pco_id') or \
                rail.result('create_potential_co').get('id')
            wbs_code_mapping = dag_run.conf.get('wbs_code_mapping', {})

            co_number = dag_run.conf['co_number']
            job_code = dag_run.conf['job_code']

            line_items = []
            existing_potential_co_line_items = rail.result(
                'find_pcco_line_items') or []

            budget_line_items = parse_budget_line_items(
                dag_run.conf.get('budget_line_items', '[]')
            )

            pco_aggregation = {}

            for budget_item in budget_line_items:
                phase = budget_item.get('phase', '')
                category = budget_item.get('category', '')
                cost_type_name = budget_item.get('cost_type_name', '')
                budget_amount = float(budget_item.get('cost_budget', 0) or 0)
                contract_amount = float(budget_item.get('contract_amount', 0) or 0)

                phase_category = build_phase_category_code(phase, category)
                if not phase_category:
                    continue

                if phase_category not in pco_aggregation:
                    pco_aggregation[phase_category] = {
                        'phase_name': budget_item.get('phase_name', ''),
                        'category_name': budget_item.get('category_name', ''),
                        'total_amount': 0.0,
                        'breakdown': []
                    }

                pco_aggregation[phase_category]['total_amount'] += contract_amount

                if budget_amount != 0:
                    pco_aggregation[phase_category]['breakdown'].append(
                        f"{cost_type_name}={budget_amount}"
                    )

            for phase_category, agg_data in pco_aggregation.items():
                if not is_non_zero_value(agg_data['total_amount']):
                    continue

                description = ', '.join(agg_data['breakdown'])

                safe_flat_code = phase_category.replace('-', '_').replace('.', '_')
                origin_id = build_pco_origin_id(job_code, co_number, safe_flat_code)

                wbs_code = f'{phase_category}.{config.revenue_cost_type}'
                wbs_code_id = wbs_code_mapping.get(wbs_code)

                if wbs_code_id is None:
                    raise RuntimeError(
                        f"Cannot create WBS code {wbs_code} in Procore"
                    )

                pco_payload = {
                    'wbs_code_id': wbs_code_id,
                    'potential_co_id': potential_co_id,
                    'type': 'budget',
                    'origin_id': origin_id,
                    'description': f'Budget: {description}',
                    'amount': str(agg_data['total_amount'])
                }

                matching_line = next(
                    (li for li in existing_potential_co_line_items if li.get(
                        'origin_id', '') == origin_id),
                    None
                )
                if matching_line:
                    pco_payload['id'] = matching_line['id']

                line_items.append(pco_payload)

            return line_items

        for_each_pcco_line_item = rail.ForEachOperator(
            task_id='for_each_pcco_line_item',
            items=build_potential_co_line_items,
            start_task='is_create_or_update_pcco_line_item',
            end_task='end_for_each_pcco_line_item'
        )

        is_create_or_update_pcco_line_item = rail.IfOperator(
            task_id='is_create_or_update_pcco_line_item',
            test="{{ result('for_each_pcco_line_item') | attr_or_default('id', '') | is_falsy }}",
            yes_task='create_potential_co_line_item',
            no_task='update_potential_co_line_item' if config.should_allow_update else 'end_for_each_pcco_line_item'
        )

        def get_potential_co_line_item(dag_run):
            current_line_item = rail.result('for_each_pcco_line_item')

            line_item_payload = {
                "project_id": dag_run.conf['project_id'],
                "line_item": {
                    "origin_id": current_line_item['origin_id'],
                    "wbs_code_id": current_line_item['wbs_code_id'],
                    "description": current_line_item['description'],
                    "amount": str(current_line_item['amount']),
                    "unit_cost": str(current_line_item['amount']),
                    "extended_type": "manual"
                }
            }

            return line_item_payload

        create_potential_co_line_item = rail.ProcoreApiOperator(
            task_id='create_potential_co_line_item',
            endpoint="/potential_change_orders/{{ result('for_each_pcco_line_item').potential_co_id }}/line_items",
            method='POST',
            data=get_potential_co_line_item
        )

        if config.should_allow_update:
            update_potential_co_line_item = rail.ProcoreApiOperator(
                task_id='update_potential_co_line_item',
                endpoint="/potential_change_orders/{{ result('for_each_pcco_line_item').potential_co_id }}/line_items/{{ \
                    result('for_each_pcco_line_item').id }}",
                method='PATCH',
                data=get_potential_co_line_item
            )

        end_for_each_pcco_line_item = rail.EmptyOperator(
            task_id='end_for_each_pcco_line_item'
        )


        assign_cop_to_pco = rail.ProcoreApiOperator(
            task_id='assign_cop_to_pco',
            endpoint="/potential_change_orders/sync",
            method='PATCH',
            query_params={
                'project_id': "{{ dag_run.conf.project_id }}",
                'contract_id': "{{ dag_run.conf.prime_contract_id }}"
            },
            data=lambda dag_run: {
                'updates': [
                    {
                        'status': 'draft' if config.should_allow_update else 'approved',
                        'origin_id': build_pco_origin_id(dag_run.conf['job_code'], dag_run.conf['co_number']),
                        'change_order_request': {
                            'change_order_package_id': dag_run.conf.get('cop_id') \
                                or rail.result('create_co_package').get('id')
                        }
                    }
                ]
            }
        )

        mark_cop_synced = rail.ProcoreApiOperator(
            task_id='mark_cop_synced',
            endpoint="/change_order_packages/{{ dag_run.conf.cop_id or result('create_co_package').get('id') }}",
            method='PATCH',
            data=lambda dag_run: {
                "change_order": {
                    "status": "draft" if config.should_allow_update else "approved",
                    "origin_id": build_cop_origin_id(dag_run.conf['job_code'], dag_run.conf['co_number'])
                },
                "project_id": dag_run.conf['project_id'],
                "contract_id": dag_run.conf['prime_contract_id']
            }
        )


        def get_rfc_budget_change_payload(dag_run):
            """Build budget change payload for a single RFC"""
            rfc_data = dag_run.conf
            job_code = rfc_data['job_code']
            co_number = rfc_data['co_number']
            wbs_code_mapping = rfc_data.get('wbs_code_mapping', {})

            budget_line_items_str = rfc_data.get('budget_line_items', '[]')
            budget_line_items = parse_budget_line_items(budget_line_items_str)

            if not budget_line_items:
                return None

            adjustment_line_items = []
            ref_counter = 1

            for item in budget_line_items:
                cost_budget = item.get('cost_budget', 0)
                if not is_non_zero_value(cost_budget):
                    continue

                flat_code = item.get('flat_code')
                if not flat_code or flat_code not in wbs_code_mapping:
                    continue

                wbs_code_id = wbs_code_mapping[flat_code]
                comment = f"CE RFC #{co_number} synced on {rfc_data['date']}"
                description = f"{item.get('phase_name', '')} - {item.get('category_name', '')} - {item.get('cost_type_name', '')}"

                adjustment_item = create_adjustment_line_item(
                    ref_counter=ref_counter,
                    amount=cost_budget,
                    comment=comment,
                    description=description,
                    wbs_code_id=wbs_code_id
                )
                adjustment_line_items.append(adjustment_item)
                ref_counter += 1

            if not adjustment_line_items:
                return None

            payload = {
                "title": co_number,
                "status": "draft" if config.should_allow_update else "approved",
                "description": f"CE Budget Change: Job - {job_code} | RFC - {co_number}",
                "adjustment_line_items": adjustment_line_items
            }
            return payload

        build_bc_payload_from_rfc = rail.PythonOperator(
            task_id='build_bc_payload_from_rfc',
            python_callable=get_rfc_budget_change_payload
        )

        check_has_budget_changes = rail.IfOperator(
            task_id='check_has_budget_changes',
            test="{{ result('build_bc_payload_from_rfc') is not none }}",
            yes_task='build_budget_change_payload',
            no_task='catch_error'
        )

        def get_budget_change_payload(dag_run):
            """Build minimal PATCH payload to update budget change amounts"""
            budget_change_id = dag_run.conf['budget_change_id']

            new_payload = rail.result('build_bc_payload_from_rfc')
            if not budget_change_id:
                return {
                    'action': 'create',
                    'payload': new_payload
                }
            existing_bc_items = dag_run.conf['budget_change_line_items']
            if not existing_bc_items or not new_payload:
                return None

            new_amounts_by_wbs = {
                str(item['wbs_code_id']): item['amount']
                for item in new_payload['adjustment_line_items']
            }

            minimal_line_items = []
            for existing_item in existing_bc_items:
                wbs_code_id = str(existing_item['wbs_code_id'])

                if wbs_code_id in new_amounts_by_wbs:
                    minimal_line_items.append({
                        'id': existing_item['id'],
                        'type': existing_item['type'],
                        'ref': str(existing_item['adjustment_number']),
                        'amount': new_amounts_by_wbs[wbs_code_id]
                    })

            update_payload = {
                'id': budget_change_id,
                'adjustment_line_items': minimal_line_items
            }

            return {
                'action': 'update',
                'id': budget_change_id,
                'payload': update_payload
            }

        build_budget_change_payload = rail.PythonOperator(
            task_id='build_budget_change_payload',
            python_callable=get_budget_change_payload
        )

        is_create_or_update = rail.IfOperator(
            task_id='is_create_or_update',
            test="{{ result('build_budget_change_payload').action == 'create' }}",
            yes_task='create_budget_change',
            no_task='update_budget_change'
        )

        create_budget_change = rail.ProcoreApiOperator(
            task_id='create_budget_change',
            endpoint='/projects/{{ dag_run.conf.project_id }}/budget_changes',
            method='POST',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data=lambda: rail.result('build_budget_change_payload')['payload']
        )

        update_budget_change = rail.ProcoreApiOperator(
            task_id='update_budget_change',
            endpoint="/projects/{{ dag_run.conf.project_id }}/budget_changes/{{ result('build_budget_change_payload').id }}",
            method='PATCH',
            query_params={
                'project_id': '{{ dag_run.conf.project_id }}'
            },
            data=lambda: rail.result('build_budget_change_payload')['payload']
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties={
                'job_code': '{{ dag_run.conf.job_code }}',
                'job_name': '{{ dag_run.conf.job_name }}',
                'co_number': '{{ dag_run.conf.co_number }}',
                'rfc_number': '{{ dag_run.conf.rfc_number }}',
                'error_message': 'Change Order sync failed - {{ get_error_message() }}'
            }
        )

        batch_task >> is_co_package_exists

        is_co_package_exists >> rail.Label('No') >> create_co_package >> is_pco_exists
        if config.should_allow_update:
            is_co_package_exists >> rail.Label('Yes') >> update_co_package >> is_pco_exists
        else:
            is_co_package_exists >> rail.Label('Yes') >> is_pco_exists
        
        is_pco_exists >> rail.Label('Create') >> create_potential_co >> find_pcco_line_items
        if config.should_allow_update:
            is_pco_exists >> rail.Label('Update') >> update_potential_co >> find_pcco_line_items
        else:
            is_pco_exists >> rail.Label('Update') >> find_pcco_line_items

        find_pcco_line_items >> for_each_pcco_line_item >> is_create_or_update_pcco_line_item
        is_create_or_update_pcco_line_item >> rail.Label('Create') >> create_potential_co_line_item >> end_for_each_pcco_line_item
        if config.should_allow_update:
            is_create_or_update_pcco_line_item >> rail.Label('Update') >> update_potential_co_line_item >> end_for_each_pcco_line_item
        else:
            is_create_or_update_pcco_line_item >> rail.Label('Update') >> end_for_each_pcco_line_item

        for_each_pcco_line_item >> end_for_each_pcco_line_item >> assign_cop_to_pco
        assign_cop_to_pco >> mark_cop_synced >> build_bc_payload_from_rfc >> check_has_budget_changes

        check_has_budget_changes >> rail.Label('Yes') >> build_budget_change_payload >> is_create_or_update
        check_has_budget_changes >> rail.Label('No') >> catch_error

        is_create_or_update >> rail.Label('Create') >> create_budget_change >> catch_error
        is_create_or_update >> rail.Label('Update') >> update_budget_change >> catch_error

        batch_task >> catch_error

    return dag


rail.for_each_instance(create_dag_instance)
