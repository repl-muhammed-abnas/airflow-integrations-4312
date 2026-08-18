from datetime import timedelta
import base64
import io
import zipfile
import rail

from procore_ce_integration.initial_setup_sync.shared_utils import build_import_file_description, normalize_ce_identifier
from procore_ce_integration.job_structure_sync.utils.xml_generator import generate_job_xml
from procore_ce_integration.job_structure_sync.utils.constants import WBSType, RESOURCE_JOBS


def create_dag_instance(config):  # pylint: disable = too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.job_structure_child_dag_id,
        description='Procore Job Structure Webhook Events Processing - Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs_child,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'aws_conn_id': config.aws_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch',
            start_task='fetch_project_data',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_project_data(response, dag_run):
            project = next((p for p in response if str(
                p['id']) == dag_run.conf['project_data']['project_id']), None)
            if not project:
                raise Exception(
                    f"Project with ID {dag_run.conf['project_data']['project_id']} not found.")

            procore_project_number = project.get('project_number', '')
            existing_origin_id = project.get('origin_id', '')
            origin_id = f"CE_{normalize_ce_identifier(procore_project_number)}" if procore_project_number else ''

            return {
                'data': project,
                'id': project['id'],
                'name': project['name'],
                'origin_id': origin_id,
                'project_number': procore_project_number,
                'existing_origin_id': existing_origin_id
            }

        fetch_project_data = rail.ProcoreApiOperator(
            task_id='fetch_project_data',
            endpoint='/projects',
            method='GET',
            query_params={
                'company_id': '{{ dag_run.conf.company_id }}',
                'filters[id]': '{{ dag_run.conf.project_data.project_id }}'
            },
            data_handler=lambda response, dag_run: get_project_data(
                response, dag_run) if response and len(response) > 0 else {}
        )

        if_project_data_found = rail.IfOperator(
            task_id='if_project_data_found',
            test=lambda: bool(rail.result('fetch_project_data')),
            yes_task='if_project_number_present',
            no_task='log_project_sync_skipped'
        )

        if_project_number_present = rail.IfOperator(
            task_id='if_project_number_present',
            test=lambda: bool(rail.result('fetch_project_data').get('project_number', '')),
            yes_task='if_duplicate_check_required',
            no_task='catch_error'
        )

        if_duplicate_check_required = rail.IfOperator(
            task_id='if_duplicate_check_required',
            test=lambda: rail.result('fetch_project_data').get('existing_origin_id', '') != rail.result('fetch_project_data')['origin_id'],
            yes_task='check_for_duplicate_project_number',
            no_task='get_project_wbs_codes'
        )

        def _check_duplicate_handler(response):
            project_number = rail.result('fetch_project_data')['project_number']
            matches = [p for p in response if p.get('project_number') == project_number]
            return {
                'duplicate_found': len(matches) > 1,
                'matching_count': len(matches)
            }

        check_for_duplicate_project_number = rail.ProcoreApiOperator(
            task_id='check_for_duplicate_project_number',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template('{{ dag_run.conf.company_id }}'),
                'filters[project_number]': rail.result('fetch_project_data')['project_number']
            },
            data_handler=_check_duplicate_handler
        )

        if_duplicate_project_number = rail.IfOperator(
            task_id='if_duplicate_project_number',
            test=lambda: rail.result('check_for_duplicate_project_number')['duplicate_found'],
            yes_task='log_duplicate_project_number',
            no_task='get_project_wbs_codes'
        )

        log_duplicate_project_number = rail.WriteLogOperator(
            task_id='log_duplicate_project_number',
            message='na',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PROJECT',
                'entity_code': rail.result('fetch_project_data')['project_number'],
                'procore_project_id': dag_run.conf['project_data']['project_id'],
                'procore_project_name': rail.result('fetch_project_data').get('name', ''),
                'error_message': (
                    f"Project sync skipped: Project Number '{rail.result('fetch_project_data')['project_number']}' "
                    f"is not unique in Procore - found {rail.result('check_for_duplicate_project_number')['matching_count']} projects "
                    f"with this number. Assign a unique Project Number in Procore."
                )
            }
        )

        get_project_wbs_codes = rail.ProcoreApiOperator(
            task_id='get_project_wbs_codes',
            endpoint='/projects/{{ dag_run.conf.project_data.project_id }}/work_breakdown_structure/wbs_codes',  # pylint: disable=line-too-long
            method='GET'
        )

        if_full_sync = rail.IfOperator(
            task_id='if_full_sync',
            test=lambda dag_run: dag_run.conf['project_data'].get('should_do_full_sync', False),
            yes_task='fetch_budget_views',
            no_task='is_budget_line_item_present'
        )

        fetch_budget_views = rail.ProcoreApiOperator(
            task_id='fetch_budget_views',
            endpoint='/budget_views',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_data.project_id }}'
            },
            data_handler=lambda response: next(
                (view['id'] for view in response if view.get('name') == config.budget_view_name),
                None
            )
        )

        if_budget_view_found = rail.IfOperator(
            task_id='if_budget_view_found',
            test=lambda: rail.result('fetch_budget_views') is not None,
            yes_task='fetch_all_budget_line_items',
            no_task='is_prime_contract_present'
        )

        fetch_all_budget_line_items = rail.ProcoreApiOperator(
            task_id='fetch_all_budget_line_items',
            endpoint=lambda: f'/budget_views/{rail.result("fetch_budget_views")}/detail_rows',
            method='GET',
            query_params={
                'project_id': '{{ dag_run.conf.project_data.project_id }}'
            }
        )

        is_budget_line_item_present = rail.IfOperator(
            task_id='is_budget_line_item_present',
            test=lambda dag_run: len(
                dag_run.conf['project_data']['budget_line_item_ids']) > 0,
            yes_task='trigger_get_budget_line_items',
            no_task='is_prime_contract_present'
        )

        trigger_get_budget_line_items = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_get_budget_line_items',
            items=lambda dag_run: dag_run.conf['project_data']['budget_line_item_ids'],
            trigger_dag_id=config.budget_line_item_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                'budget_line_item_id': item,
                'company_id': dag_run.conf['company_id'],
                'project_id': dag_run.conf['project_data']['project_id'],
                'project_name': rail.result('fetch_project_data').get('name', ''),
            }
        )

        wait_for_get_budget_line_items_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_get_budget_line_items_completion',
            dag_runs='{{ result("trigger_get_budget_line_items") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_budget_line_items_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_budget_line_items_data',
            dag_runs="{{ result('trigger_get_budget_line_items') }}",
            dagrun_task_id='get_budget_line_item',
            flatten=True
        )

        def get_formatted_budget_line_items_data(dag_run):
            if dag_run.conf['project_data'].get('should_do_full_sync', False):
                budget_line_items_data = rail.result('fetch_all_budget_line_items') or []
            else:
                budget_line_items_data = rail.result('gather_budget_line_items_data')
            project_wbs_codes = rail.result('get_project_wbs_codes')
            cost_code_ids = dag_run.conf['project_data'].get(
                'cost_code_ids', [])
            budgets = {}
            budgets_not_synced = []

            for data in budget_line_items_data:
                path_code, cost_type = data['wbs_code']['flat_code'].split(
                    '.', 1)

                cost_code_segment = None
                for entry in project_wbs_codes:
                    for segment_item in entry['segment_items']:
                        if segment_item['path_code'] == path_code and segment_item.get('segment', {}).get('type') == 'cost_code':
                            cost_code_id = segment_item['id']
                            cost_code_segment = segment_item
                            break
                    if cost_code_segment:
                        break

                if cost_code_segment and cost_code_segment.get('parent_id') is None and rail.result('search_job_in_ce')['wbs_type'] == WBSType.JOB_PHASE_CAT:
                    budgets_not_synced.append(data['id'])

                if cost_code_id not in cost_code_ids:
                    cost_code_ids.append(cost_code_id)
                budget = {
                    'path_code': path_code,
                    'cost_type': cost_type,
                    'quantity': data['quantity'],
                    'unit_cost': data['unit_cost'],
                    'original_budget_amount': data['original_budget_amount'],
                    'id': data['id']
                }
                if cost_code_id not in budgets:
                    budgets[cost_code_id] = [budget]
                else:
                    budgets[cost_code_id].append(budget)

            dag_run.conf['project_data']['cost_code_ids'] = cost_code_ids
            dag_run.conf['budgets_not_synced'] = budgets_not_synced
            return budgets

        formatted_budget_line_items_data = rail.PythonOperator(
            task_id='formatted_budget_line_items_data',
            python_callable=get_formatted_budget_line_items_data
        )

        is_prime_contract_present = rail.IfOperator(
            task_id='is_prime_contract_present',
            test=lambda dag_run: dag_run.conf['project_data'].get(
                'has_prime_contract', False),
            yes_task='trigger_get_prime_contract_line_items',
            no_task='get_computerease_cost_types'
        )

        trigger_get_prime_contract_line_items = rail.TriggerDagRunOperator(
            task_id='trigger_get_prime_contract_line_items',
            trigger_dag_id=config.prime_contract_line_items_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'project_id': dag_run.conf['project_data']['project_id'],
                'project_number': rail.result('fetch_project_data')['project_number'],
                'project_name': rail.result('fetch_project_data').get('name', ''),
                'wbs_type': rail.result('search_job_in_ce')['wbs_type'],
                'contract_by_category': rail.result('search_job_in_ce')['contract_by_category']
            }
        )

        wait_for_prime_contract_line_items_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_prime_contract_line_items_completion',
            dag_runs='{{ result("trigger_get_prime_contract_line_items") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_prime_contract_line_items_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_prime_contract_line_items_data',
            dag_runs='{{ result("trigger_get_prime_contract_line_items") }}',
            dagrun_task_id='parse_line_items'
        )

        get_computerease_cost_types = rail.ComputereaseAPIOperator(
            task_id='get_computerease_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda response: {
                item['reference']: item['code']
                for item in response.get('data', [])
                if item.get('code') and item.get('reference')
            }
        )

        def get_contract_data():
            # Extract contract data from gathered contract line items data
            gathered_contract_data = rail.result(
                'gather_prime_contract_line_items_data')
            if gathered_contract_data and len(gathered_contract_data) > 0:
                result = gathered_contract_data[0]
                return result if result is not None else {}
            return {}

        def get_contract_cost_code_ids():
            # Extract cost_code_ids from contract data
            return get_contract_data().get('cost_code_ids', [])

        def get_contract_amounts():
            # Extract contract amounts from contract data
            return get_contract_data().get('contracts', {})

        def filter_required_cost_codes(response, dag_run):
            # To allow full sync when required, or when prime contract changed (to handle removed line items)
            if dag_run.conf.get('sync_all_cost_codes', False) or (config.support_contract_line_item_removal and dag_run.conf['project_data'].get('has_prime_contract', False)):
                return response
            required_cost_code_ids = dag_run.conf['project_data'].get(
                'cost_code_ids', [])
            contract_line_item_cost_code_ids = get_contract_cost_code_ids()
            all_required_cost_code_ids = set(required_cost_code_ids) | set(
                contract_line_item_cost_code_ids)
            return [cc for cc in response if str(cc['id']) in all_required_cost_code_ids or cc['id'] in all_required_cost_code_ids]

        fetch_cost_code_segments = rail.ProcoreApiOperator(
            task_id='fetch_cost_code_segments',
            endpoint='/projects/{{ dag_run.conf.project_data.project_id }}/work_breakdown_structure/segments/{{ dag_run.conf.cost_code_segment_id }}/segment_items',  # pylint: disable=line-too-long
            method='GET',
            data_handler=lambda response, dag_run: filter_required_cost_codes(  # pylint: disable = unnecessary-lambda
                response, dag_run)
        )

        def derive_wbs_type_from_cost_codes():
            # When job is not yet in CE, derive WBS type from Procore's cost code structure:
            # nested codes (any with a parent) indicate JOB_PHASE_CAT, flat codes indicate JOB_CAT.
            cost_codes = rail.result('fetch_cost_code_segments') or []
            has_multiple_levels = False
            for cc in cost_codes:
                if len(cc.get('path_ids', [])) > 2:
                    raise Exception(
                        f"Cost code hierarchy deeper than 2 levels not supported: {cc.get('path_code')}")
                if cc.get('parent_id') is not None:
                    has_multiple_levels = True
            return WBSType.JOB_PHASE_CAT if has_multiple_levels else WBSType.JOB_CAT

        search_job_in_ce = rail.ComputereaseAPIOperator(
            task_id='search_job_in_ce',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {
                'code': normalize_ce_identifier(extract_project_number(rail.result('fetch_project_data')['origin_id']))
            },
            data_handler=lambda response: {
                'wbs_type': response['data'][0]['wbs_type'] if response.get('data') else derive_wbs_type_from_cost_codes(),
                'contract_by_category': response['data'][0]['contract_by_cat'] if response.get('data') else None,
                'job_found': len(response.get('data', [])) > 0
            }
        )

        def extract_project_number(origin_id):
            if origin_id and origin_id.startswith('CE_'):
                return origin_id[3:]
            return origin_id

        def split_address_to_dict(address_str):
            # Split address: fill line1 & line2 with 30 chars max, put ALL remaining in line3
            if not address_str:
                return {}

            max_length = config.address_max_length
            words = address_str.split()
            lines, current = [], ""

            for word in words:
                test_line = current + " " + word if current else word
                # If this would exceed 30 chars and we're not on the last line
                if len(test_line) > max_length and len(lines) < (config.address_max_lines - 1):
                    if current:
                        lines.append(current)
                        current = word
                    else:
                        # Single word too long for line 1 or 2, put it on next line
                        lines.append("")
                        current = word
                else:
                    current = test_line

            # Add remaining text (could exceed 30 chars if it's line 3)
            if current:
                lines.append(current)

            return {f'address{i+1}': line for i, line in enumerate(lines) if line}

        def parse_data_for_xml(dag_run):
            project = rail.result('fetch_project_data')['data']
            cost_codes_data = rail.result('fetch_cost_code_segments')
            computerease_cost_types = rail.result(
                'get_computerease_cost_types')
            budgets = (rail.result('formatted_budget_line_items_data') or {}) if (
                len(dag_run.conf['project_data']['budget_line_item_ids']) > 0 or
                dag_run.conf['project_data'].get('should_do_full_sync', False)
            ) else {}
            budgets_not_synced = dag_run.conf['budgets_not_synced'] if 'budgets_not_synced' in dag_run.conf else [
            ]
            contract_amounts = get_contract_amounts() if dag_run.conf['project_data'].get(
                'has_prime_contract', False) else {}

            origin_id = rail.result('fetch_project_data')['origin_id']

            # Prepare project data
            state_code = project.get('state_code', '')
            project_info = {
                'name': project.get('name', ''),
                'project_number': extract_project_number(origin_id),
                'active': project.get('active', True),
                **split_address_to_dict(project.get('address', '')),
                'city': project.get('city', ''),
                'state_code': state_code if len(str(state_code)) <= 2 else '',
                'zip': project.get('zip', ''),
                'start_date': project.get('start_date', ''),
                'completion_date': project.get('completion_date', '')
            }

            # Prepare cost codes data
            cost_codes_info = {}
            for cost_code in cost_codes_data:
                name = cost_code.get('name', '')
                code = cost_code.get('code', '')
                path_code = cost_code.get('path_code', '')
                cost_code_id = str(cost_code['id'])
                budgets_to_sync = []
                if cost_code_id in budgets:
                    budget_line_items = budgets[str(cost_code_id)]
                    for budget_line_item in budget_line_items:
                        if budget_line_item['cost_type'] in computerease_cost_types:
                            budget_cost_type = budget_line_item['cost_type']
                            budget = {
                                'number': computerease_cost_types[budget_cost_type],
                                'hours': budget_line_item['quantity'],
                                'cost': budget_line_item['original_budget_amount']
                            }
                            budgets_to_sync.append(budget)
                        else:
                            budgets_not_synced.append(budget_line_item['id'])

                if cost_code.get('parent_id') is None:  # Top-level cost code
                    if code not in cost_codes_info:
                        cost_codes_info[code] = {
                            'code': code,
                            'name': name,
                            'children': []
                        }
                    else:
                        cost_codes_info[code]['name'] = name
                    if len(budgets_to_sync) > 0:
                        cost_codes_info[code]['budgets'] = budgets_to_sync
                    if cost_code_id in contract_amounts or (config.support_contract_line_item_removal and dag_run.conf['project_data'].get('has_prime_contract', False)):
                        cost_codes_info[code]['contractamount'] = contract_amounts.get(cost_code_id, 0)

                else:  # Sub-level cost code
                    if len(cost_code.get('path_ids', [])) > 2:
                        raise Exception(
                            f"Cost code hierarchy deeper than 2 levels not supported: {path_code}")
                    parent_code = path_code[:-len(code)-1]
                    parent_path_code_str = cost_code.get('path_codes', [''])[0]
                    parent_prefix = f"{parent_code} - "
                    parent_name = parent_path_code_str[len(parent_prefix):] if parent_path_code_str.startswith(parent_prefix) else ''
                    if parent_code not in cost_codes_info:
                        cost_codes_info[parent_code] = {
                            'code': parent_code,
                            'name': parent_name,
                            'children': []
                        }
                    child_data = {
                        'code': code,
                        'name': name
                    }
                    if len(budgets_to_sync) > 0:
                        child_data['budgets'] = budgets_to_sync
                    if cost_code_id in contract_amounts or (config.support_contract_line_item_removal and dag_run.conf['project_data'].get('has_prime_contract', False)):
                        child_data['contractamount'] = contract_amounts.get(cost_code_id, 0)
                    cost_codes_info[parent_code]['children'].append(child_data)

            dag_run.conf['budgets_not_synced'] = budgets_not_synced
            return {
                'project': project_info,
                'cost_codes': cost_codes_info,
                'wbs_type': rail.result('search_job_in_ce')['wbs_type']
            }

        prepare_data = rail.PythonOperator(
            task_id='prepare_data',
            python_callable=parse_data_for_xml
        )

        def get_effective_contract_by_category(wbs_type):
            # Prime contract line items take precedence (detected and validated against CE).
            # Fall back to CE stored value if prime contract dag didn't run or had no line items.
            # For JOB_CAT with no prior data, always True. For others, return None so CE sets its own default.
            contract_by_category_from_pc = get_contract_data().get('contract_by_category')
            ce_stored = (rail.result('search_job_in_ce') or {}).get('contract_by_category')
            if contract_by_category_from_pc is not None:
                return contract_by_category_from_pc
            if ce_stored is not None:
                return ce_stored
            return True if wbs_type == WBSType.JOB_CAT else None

        def generate_xml_zip_and_encode():
            prepared_data = rail.result('prepare_data')
            project = prepared_data['project']
            cost_codes = prepared_data['cost_codes']
            wbs_type = prepared_data['wbs_type']

            effective_contract_by_category = get_effective_contract_by_category(wbs_type)

            # Generate raw XML
            xml_and_logs = generate_job_xml(
                project, cost_codes, wbs_type, effective_contract_by_category)
            raw_xml = xml_and_logs['xml']
            logs = list(filter(lambda x: x.get('errors'), xml_and_logs['logs']))
            errors_and_warnings = list(filter(lambda x: x.get('errors')
                        or x.get('warnings'), xml_and_logs['logs']))
            if not raw_xml:
                return {
                    'encoded_data': None,
                    'project_number': project['project_number'],
                    'logs': logs,
                    'errors_and_warnings': errors_and_warnings
                }
            description = build_import_file_description(RESOURCE_JOBS, project['project_number'])

            # Create a zip file in memory
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr('job_structure.xml', raw_xml)

            zip_data = zip_buffer.getvalue()

            # Base64 encode the zip data
            base64_encoded = base64.b64encode(zip_data).decode('utf-8')

            # Free memory
            zip_data = None
            zip_buffer.close()

            return {
                'raw_xml': raw_xml,
                'description': description,
                'encoded_data': base64_encoded,
                'project_number': project['project_number'],
                'logs': logs
            }

        generate_xml = rail.PythonOperator(
            task_id='generate_xml',
            python_callable=generate_xml_zip_and_encode
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test=lambda: len(rail.result("generate_xml")['logs']) > 0,
            yes_task='write_validation_logs',
            no_task='if_encoded_xml_present'
        )

        def get_err_message(item):
            error_message = "; ".join(item['errors']) if item['errors'] else ""
            warnings_message = "; ".join(
                item['warnings']) if item['warnings'] else ""
            message = ''
            if error_message:
                message = f"{item['type']} '{item['identifier']}' and/or its children could not be synced due to: {error_message}"
            elif warnings_message:
                message = f"{item['type']} '{item['identifier']}' synced with warnings: {warnings_message}. Excess characters will be truncated"
            return message

        write_validation_logs = rail.WriteLogOperator(
            task_id='write_validation_logs',
            items=lambda: rail.result('generate_xml')['logs'],
            message='na',
            severity='Error/Exception',
            properties=lambda item, dag_run: {
                'entity_type': (item['type']).upper(),
                'entity_code': item['identifier'],
                'procore_project_id': dag_run.conf['project_data']['project_id'],
                'procore_project_name': rail.result('fetch_project_data')['name'],
                'error_message': get_err_message(item)
            }
        )

        if_encoded_xml_present = rail.IfOperator(
            task_id='if_encoded_xml_present',
            test=lambda: rail.result('generate_xml')[
                'encoded_data'] is not None,
            yes_task='computerease_import_sync.get_import_file_id',
            no_task='catch_error'
        )


        get_import_file_id, import_sync_finish = rail.computerease_import_sync(
            group_id='computerease_import_sync',
            import_type=RESOURCE_JOBS,
            description=lambda: rail.result('generate_xml')['description'],
            import_data=lambda: rail.result('generate_xml')['encoded_data']
        )

        def get_project_context(dag_run):
            # On send path only; presence in the gather means import was sent.
            project = rail.result('fetch_project_data') or {}
            import_uuid = rail.result('computerease_import_sync.get_import_file_id')
            if not import_uuid:
                created = rail.result('computerease_import_sync.create_import_file') or {}
                import_uuid = (created.get('data') or {}).get('uuid', '')
            return {
                'procore_project_id': dag_run.conf['project_data']['project_id'],
                'project_number': project.get('project_number', ''),
                'origin_id': project.get('origin_id', ''),
                'existing_origin_id': project.get('existing_origin_id', ''),
                'import_uuid': import_uuid or ''
            }

        build_project_context = rail.PythonOperator(
            task_id='build_project_context',
            python_callable=get_project_context
        )

        check_not_synced_budgets = rail.IfOperator(
            task_id='check_not_synced_budgets',
            test=lambda dag_run: len(dag_run.conf['budgets_not_synced']) > 0,
            yes_task='log_not_synced_budgets',
            no_task='catch_error'
        )

        log_not_synced_budgets = rail.WriteLogOperator(
            task_id='log_not_synced_budgets',
            message='na',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'BUDGET',
                'budget_line_item_ids': dag_run.conf['budgets_not_synced'],
                'procore_project_id': dag_run.conf['project_data']['project_id'],
                'procore_project_name': rail.result('fetch_project_data').get('name', ''),
                'error_message': 'No matching cost type found in Computerease or Budget created at Phase Level in Procore'
            }
        )

        log_project_sync_skipped = rail.WriteLogOperator(
            task_id='log_project_sync_skipped',
            message='na',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PROJECT',
                'entity_code': rail.result('fetch_project_data').get('project_number') or dag_run.conf['project_data']['project_id'],
                'procore_project_id': dag_run.conf['project_data']['project_id'],
                'procore_project_name': rail.result('fetch_project_data').get('name', '') if rail.result('fetch_project_data') else '',
                'error_message': 'Project not synced to CE: Project not found in Procore, or it is inactive.'
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PROJECT',
                'entity_code': (rail.result('fetch_project_data') or {}).get('project_number') or dag_run.conf['project_data']['project_id'],
                'procore_project_id': dag_run.conf['project_data']['project_id'],
                'procore_project_name': rail.result('fetch_project_data').get('name', '') if rail.result('fetch_project_data') else '',
                'error_message': 'Project and/or its children could not be synced - {{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> catch_error
        batch_task >> fetch_project_data >> if_project_data_found

        if_project_data_found >> rail.Label('Yes') >> if_project_number_present
        if_project_data_found >> rail.Label('No') >> log_project_sync_skipped >> catch_error

        if_project_number_present >> rail.Label('Yes') >> if_duplicate_check_required
        if_project_number_present >> rail.Label('No') >> catch_error

        if_duplicate_check_required >> rail.Label('Yes') >> check_for_duplicate_project_number >> if_duplicate_project_number
        if_duplicate_check_required >> rail.Label('No') >> get_project_wbs_codes

        if_duplicate_project_number >> rail.Label('Yes') >> log_duplicate_project_number >> catch_error
        if_duplicate_project_number >> rail.Label('No') >> get_project_wbs_codes

        get_project_wbs_codes >> fetch_cost_code_segments >> search_job_in_ce >> if_full_sync
        if_full_sync >> rail.Label('Yes') >> fetch_budget_views >> if_budget_view_found
        if_budget_view_found >> rail.Label('Yes') >> fetch_all_budget_line_items >> formatted_budget_line_items_data
        if_budget_view_found >> rail.Label('No') >> is_prime_contract_present
        if_full_sync >> rail.Label('No') >> is_budget_line_item_present

        is_budget_line_item_present >> rail.Label('Yes') >> trigger_get_budget_line_items >> wait_for_get_budget_line_items_completion >> gather_budget_line_items_data \
            >> formatted_budget_line_items_data >> is_prime_contract_present
        is_budget_line_item_present >> rail.Label(
            'No') >> is_prime_contract_present

        is_prime_contract_present >> rail.Label(
            'Yes') >> trigger_get_prime_contract_line_items >> wait_for_prime_contract_line_items_completion
        wait_for_prime_contract_line_items_completion >> gather_prime_contract_line_items_data >> get_computerease_cost_types
        is_prime_contract_present >> rail.Label(
            'No') >> get_computerease_cost_types

        get_computerease_cost_types >> prepare_data >> generate_xml >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_validation_logs >> if_encoded_xml_present
        if_logs_present >> rail.Label('No') >> if_encoded_xml_present

        if_encoded_xml_present >> rail.Label('No') >> catch_error
        if_encoded_xml_present >> rail.Label('Yes') >> get_import_file_id
        import_sync_finish >> build_project_context >> check_not_synced_budgets

        check_not_synced_budgets >> rail.Label(
            'Yes') >> log_not_synced_budgets >> catch_error
        check_not_synced_budgets >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
