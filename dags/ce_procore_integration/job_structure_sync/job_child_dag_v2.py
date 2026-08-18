from datetime import timedelta
import rail
import pycountry
from ce_procore_integration.job_structure_sync.utils.job_parser import parse_job_data
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):  # pylint: disable=too-many-statements
    if not config.job_child_dag_v2_id:
        return None
    with rail.create_airflow_dag(
        dag_id=config.job_child_dag_v2_id,
        description='Computerease to Procore job sync Child DAG v2',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.job_child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_had_duplicates',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        if_had_duplicates = rail.IfOperator(
            task_id='if_had_duplicates',
            test=lambda dag_run: bool(dag_run.conf.get('had_duplicates_in_procore')),
            yes_task='log_duplicate_project_error',
            no_task='check_if_need_job_fetch'
        )

        log_duplicate_project_error = rail.WriteLogOperator(
            task_id='log_duplicate_project_error',
            message='Multiple Procore projects found',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'JOB',
                'entity_code': dag_run.conf.get('job_code', ''),
                'full_code': dag_run.conf.get('job_code', ''),
                'entity_name': '',
                'error_message': f"Job not synced - Multiple Procore projects found with project number: {dag_run.conf.get('job_code', '')}."
            }
        )

        check_if_need_job_fetch = rail.IfOperator(
            task_id='check_if_need_job_fetch',
            test=lambda dag_run: dag_run.conf.get('job_data') is None,
            yes_task='fetch_job_data',
            no_task='prepare_job_data'
        )

        fetch_job_data = rail.ComputereaseAPIOperator(
            task_id='fetch_job_data',
            endpoint='/catalog/job',
            request_method='GET',
            query_params={
                'code': '{{ dag_run.conf.job_code }}'
            }
        )

        def get_job_data_for_sync(dag_run):
            if dag_run.conf.get('job_data'):
                return dag_run.conf['job_data']
            job_raw_data = rail.result('fetch_job_data').get('data', [])
            if job_raw_data:
                return parse_job_data(
                    job_raw_data[0],
                    dag_run.conf.get('department_lookup', {}),
                    dag_run.conf.get('project_template_udf_id')
                )
            raise ValueError(
                f"Job not found in CE: {dag_run.conf['job_code']}.")

        prepare_job_data = rail.PythonOperator(
            task_id='prepare_job_data',
            python_callable=get_job_data_for_sync
        )

        def validate_state_code(state_code, country_code='US'):
            if not state_code:
                return ""
            try:
                state_code = state_code.strip().upper()
                subdivisions = pycountry.subdivisions.get(
                    country_code=country_code)
                for subdivision in subdivisions:
                    if subdivision.code.split('-')[-1].upper() == state_code:
                        return state_code
                return ""
            except:  # pylint: disable=bare-except
                return ""

        def get_project_template_id(project_templates_lookup, project_template_udf_value):
            if project_template_udf_value and project_templates_lookup.get(project_template_udf_value):
                return project_templates_lookup[project_template_udf_value]
            return project_templates_lookup.get(config.default_project_template_name, "")

        def build_sync_project_payload(dag_run):
            job_data = rail.result('prepare_job_data')
            procore_project_id = dag_run.conf.get('procore_project_id')

            if not job_data.get('description'):
                raise ValueError("Job Name is required for sync.")

            origin_id = f"CE_{job_data.get('code', '')}" if job_data.get(
                'code') else None

            project_templates_lookup = dag_run.conf.get(
                'project_templates_lookup', {})
            project_template_udf_value = job_data.get(
                'project_template_udf_value', '')
            project_template_id = get_project_template_id(
                project_templates_lookup, project_template_udf_value)

            payload = {
                "company_id": dag_run.conf['procore_company_id'],
                "updates": [
                    {
                        "origin_id": origin_id,
                        "project_template_id": project_template_id,
                        "name": job_data.get('description', ''),
                        "project_number": job_data.get('code', ''),
                        "active": job_data.get('status', True),
                        "address": job_data.get('address', ''),
                        "city": job_data.get('city', ''),
                        "state_code": validate_state_code(job_data.get('state', ''), config.country_code),
                        "zip": job_data.get('zipcode', ''),
                        "start_date": job_data.get('jobdate_open', ''),
                        "completion_date": job_data.get('jobdate_due', ''),
                        "department_ids": job_data.get('department_ids', []),
                        "country_code": config.country_code,
                        "enable_copy_of_standard_cost_codes": config.enable_copy_of_standard_cost_codes,
                    }
                ]
            }
            if procore_project_id:
                payload['updates'][0]['id'] = procore_project_id

            return payload

        sync_project_to_procore = rail.ProcoreApiOperator(
            task_id='sync_project_to_procore',
            endpoint='/projects/sync',
            method='PATCH',
            data=build_sync_project_payload
        )

        check_if_successful_sync_for_job = rail.IfOperator(
            task_id='check_if_successful_sync_for_job',
            test='{{ result("sync_project_to_procore").entities | length > 0 }}',
            yes_task='fetch_cost_codes',
            no_task='log_sync_success_with_exceptions'
        )

        fetch_cost_codes = rail.ProcoreApiOperator(
            task_id='fetch_cost_codes',
            endpoint='/cost_codes',
            query_params=lambda: {
                'project_id': rail.result('sync_project_to_procore')['entities'][0]['id']
            },
            method='GET',
            data_handler=lambda response: {
                'top_level': {
                    normalize_ce_identifier(item['code']): item['id']
                    for item in (response or [])
                    if item.get('parent', {}).get('id') is None and item.get('code') and item.get('id')
                },
                'child_level': {
                    f"{item['parent']['id']}:{normalize_ce_identifier(item['code'])}": item['id']
                    for item in (response or [])
                    if item.get('parent', {}).get('id') is not None and item.get('code') and item.get('id')
                },
                'raw': response
            }
        )

        check_if_has_phases = rail.IfOperator(
            task_id='check_if_has_phases',
            test=lambda dag_run: len(dag_run.conf.get('phases', {})) > 0,
            yes_task='bulk_create_phases',
            no_task='check_if_has_any_categories'
        )

        def build_bulk_phases_payload(dag_run):
            top_level_lookup = rail.result('fetch_cost_codes').get('top_level', {})
            cost_codes = []
            for phase_code, phase_entry in dag_run.conf.get('phases', {}).items():
                phase_data = phase_entry.get('phase_data')
                item = {
                    'origin_id': f"CE_{dag_run.conf['job_code']}_{phase_code}",
                    'code': phase_code,
                    'parent_id': None
                }
                if phase_data is not None:
                    item['name'] = phase_data.get('description') or phase_code
                if phase_code in top_level_lookup:
                    item['id'] = top_level_lookup[phase_code]
                cost_codes.append(item)
            return {'updates': cost_codes}

        bulk_create_phases = rail.ProcoreApiOperator(
            task_id='bulk_create_phases',
            endpoint='/cost_codes/sync',
            method='PATCH',
            query_params=lambda: {
                'project_id': rail.result('sync_project_to_procore')['entities'][0]['id']
            },
            data=build_bulk_phases_payload,
            data_handler=lambda response: {
                'phase_id_lookup': {
                    normalize_ce_identifier(item['code']): item['id']
                    for item in (response or {}).get('entities', [])
                    if item.get('code') and item.get('id')
                },
                'errors': (response or {}).get('errors', []),
                'raw': response
            }
        )

        def format_cost_code_error(err):
            try:
                parts = []
                for k, v in err.get('errors', {}).items():
                    msg = ', '.join(str(m) for m in v) if isinstance(v, list) else str(v)
                    parts.append(f"{k}: {msg}")
                return '; '.join(parts)
            except Exception:  # pylint: disable=broad-except
                return str(err)

        log_phase_sync_exceptions = rail.WriteLogOperator(
            task_id='log_phase_sync_exceptions',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'entity_type': 'PHASE',
                    'entity_code': err.get('code', ''),
                    'full_code': f"{dag_run.conf.get('job_code', '')}.{err.get('code', '')}",
                    'entity_name': err.get('name', ''),
                    'error_message': f"Phase not synced - {format_cost_code_error(err)}; and subsequent categories if any also skipped"
                }
                for err in (rail.result('bulk_create_phases') or {}).get('errors', [])
            ],
            properties=lambda item: item
        )

        if_phase_sync_errors = rail.IfOperator(
            task_id='if_phase_sync_errors',
            test=lambda: len((rail.result('bulk_create_phases') or {}).get('errors', [])) > 0,
            yes_task='log_phase_sync_exceptions',
            no_task='check_if_has_any_categories'
        )

        check_if_has_any_categories = rail.IfOperator(
            task_id='check_if_has_any_categories',
            test=lambda dag_run: (
                any(phase.get('categories') for phase in dag_run.conf.get('phases', {}).values())
                or len(dag_run.conf.get('direct_categories', {})) > 0
            ),
            yes_task='prepare_bulk_categories_payload',
            no_task='trigger_prime_contract_sync'
        )

        def build_bulk_categories_payload(dag_run):
            child_level_lookup = rail.result('fetch_cost_codes').get('child_level', {})
            top_level_lookup = rail.result('fetch_cost_codes').get('top_level', {})
            phase_id_lookup = (rail.result('bulk_create_phases') or {}).get('phase_id_lookup', {})
            cost_codes = []

            for phase_code, phase_entry in dag_run.conf.get('phases', {}).items():
                phase_id = phase_id_lookup.get(phase_code) or top_level_lookup.get(phase_code)
                for cat_code, cat_entry in phase_entry.get('categories', {}).items():
                    if phase_id is None:
                        continue
                    cat_data = cat_entry.get('category_data')
                    name = ((cat_data or {}).get('description') or '').strip() or cat_code
                    item = {
                        'origin_id': f"CE_{dag_run.conf['job_code']}_{phase_code}_{cat_code}",
                        'code': cat_code,
                        'name': name,
                        'parent_id': phase_id
                    }
                    existing_id = child_level_lookup.get(f"{phase_id}:{cat_code}")
                    if existing_id:
                        item['id'] = existing_id
                    cost_codes.append(item)

            for cat_code, cat_entry in dag_run.conf.get('direct_categories', {}).items():
                cat_data = cat_entry.get('category_data')
                name = ((cat_data or {}).get('description') or '').strip() or cat_code
                item = {
                    'origin_id': f"CE_{dag_run.conf['job_code']}_{cat_code}",
                    'code': cat_code,
                    'name': name,
                    'parent_id': None
                }
                if cat_code in top_level_lookup:
                    item['id'] = top_level_lookup[cat_code]
                cost_codes.append(item)

            return {'updates': cost_codes}

        prepare_bulk_categories_payload = rail.PythonOperator(
            task_id='prepare_bulk_categories_payload',
            python_callable=build_bulk_categories_payload
        )

        if_has_categories_to_sync = rail.IfOperator(
            task_id='if_has_categories_to_sync',
            test='{{ result("prepare_bulk_categories_payload")["updates"] | length > 0 }}',
            yes_task='bulk_create_categories',
            no_task='trigger_prime_contract_sync'
        )

        bulk_create_categories = rail.ProcoreApiOperator(
            task_id='bulk_create_categories',
            endpoint='/cost_codes/sync',
            method='PATCH',
            query_params=lambda: {
                'project_id': rail.result('sync_project_to_procore')['entities'][0]['id']
            },
            data=lambda: rail.result('prepare_bulk_categories_payload'),
            data_handler=lambda response: {
                'errors': (response or {}).get('errors', []),
                'raw': response
            }
        )

        def _get_category_full_code(job_code, err):
            try:
                cat_code = err.get('code', '')
                parent_id = err.get('parent_id')
                if parent_id:
                    phase_id_lookup = (rail.result('bulk_create_phases') or {}).get('phase_id_lookup', {})
                    top_level_lookup = rail.result('fetch_cost_codes').get('top_level', {})
                    id_to_code = {v: k for k, v in {**top_level_lookup, **phase_id_lookup}.items()}
                    phase_code = id_to_code.get(parent_id, '')
                    return f"{job_code}.{phase_code}.{cat_code}" if phase_code else f"{job_code}.{cat_code}"
                return f"{job_code}.{cat_code}"
            except Exception:  # pylint: disable=broad-except
                return f"{job_code}.{err.get('code', '') if isinstance(err, dict) else ''}"

        log_category_sync_exceptions = rail.WriteLogOperator(
            task_id='log_category_sync_exceptions',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'entity_type': 'CATEGORY',
                    'entity_code': err.get('code', ''),
                    'full_code': _get_category_full_code(dag_run.conf.get('job_code', ''), err),
                    'entity_name': err.get('name', ''),
                    'error_message': f"Category not synced - {format_cost_code_error(err)}"
                }
                for err in (rail.result('bulk_create_categories') or {}).get('errors', [])
            ],
            properties=lambda item: item
        )

        if_category_sync_errors = rail.IfOperator(
            task_id='if_category_sync_errors',
            test=lambda: len((rail.result('bulk_create_categories') or {}).get('errors', [])) > 0,
            yes_task='log_category_sync_exceptions',
            no_task='trigger_prime_contract_sync'
        )

        trigger_prime_contract_sync = rail.TriggerDagRunOperator(
            task_id='trigger_prime_contract_sync',
            trigger_dag_id=config.prime_contract_child_dag_id,
            conf=lambda dag_run: {
                'job_code': dag_run.conf['job_code'],
                'customer_name': rail.result('prepare_job_data').get('customer_name', ''),
                'customer_code': rail.result('prepare_job_data').get('customer_code', ''),
                'job_status': rail.result('prepare_job_data').get('status', True),
                'procore_project_id': rail.result('sync_project_to_procore')['entities'][0]['id'],
                'procore_company_id': dag_run.conf['procore_company_id']
            }
        )

        wait_for_prime_contract_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_prime_contract_completion',
            dag_runs='{{ result("trigger_prime_contract_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_error_message(errors):
            if not errors or not isinstance(errors, dict):
                return ""
            messages = []
            for key, msgs in errors.items():
                if isinstance(msgs, list):
                    msg_str = ", ".join(str(m) for m in msgs)
                else:
                    msg_str = str(msgs)
                messages.append(f"{key}: {msg_str}")
            return "Project not created/updated due to - " + "; ".join(messages)

        log_sync_success_with_exceptions = rail.WriteLogOperator(
            task_id='log_sync_success_with_exceptions',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'entity_type': 'JOB',
                    'entity_code': rail.result('prepare_job_data').get('code', ''),
                    'full_code': rail.result('prepare_job_data').get('code', ''),
                    'entity_name': rail.result('prepare_job_data').get('description', ''),
                    'error_message': get_error_message(err.get('errors', {}))
                }
                for err in rail.result('sync_project_to_procore').get('errors', [])
            ],
            properties=lambda item: item
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'JOB',
                'entity_code': rail.result('prepare_job_data').get('code', '') if rail.result('prepare_job_data') else dag_run.conf.get('job_code', ''),
                'full_code': rail.result('prepare_job_data').get('code', '') if rail.result('prepare_job_data') else dag_run.conf.get('job_code', ''),
                'entity_name': rail.result('prepare_job_data').get('description', '') if rail.result('prepare_job_data') else '',
                'error_message': "Job not synced - {{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> if_had_duplicates

        if_had_duplicates >> rail.Label('Yes') >> log_duplicate_project_error >> catch_error
        if_had_duplicates >> rail.Label('No') >> check_if_need_job_fetch

        check_if_need_job_fetch >> rail.Label('Yes') >> fetch_job_data >> prepare_job_data
        check_if_need_job_fetch >> rail.Label('No') >> prepare_job_data
        prepare_job_data >> sync_project_to_procore >> check_if_successful_sync_for_job

        check_if_successful_sync_for_job >> rail.Label(
            'Yes') >> fetch_cost_codes >> check_if_has_phases
        check_if_successful_sync_for_job >> rail.Label(
            'No') >> log_sync_success_with_exceptions >> catch_error

        check_if_has_phases >> rail.Label(
            'Yes') >> bulk_create_phases >> if_phase_sync_errors
        if_phase_sync_errors >> rail.Label(
            'Yes') >> log_phase_sync_exceptions >> check_if_has_any_categories
        if_phase_sync_errors >> rail.Label('No') >> check_if_has_any_categories
        check_if_has_phases >> rail.Label('No') >> check_if_has_any_categories

        check_if_has_any_categories >> rail.Label(
            'Yes') >> prepare_bulk_categories_payload >> if_has_categories_to_sync
        check_if_has_any_categories >> rail.Label(
            'No') >> trigger_prime_contract_sync

        if_has_categories_to_sync >> rail.Label(
            'Yes') >> bulk_create_categories >> if_category_sync_errors
        if_category_sync_errors >> rail.Label(
            'Yes') >> log_category_sync_exceptions >> trigger_prime_contract_sync
        if_category_sync_errors >> rail.Label('No') >> trigger_prime_contract_sync
        if_has_categories_to_sync >> rail.Label('No') >> trigger_prime_contract_sync

        trigger_prime_contract_sync >> wait_for_prime_contract_completion >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
