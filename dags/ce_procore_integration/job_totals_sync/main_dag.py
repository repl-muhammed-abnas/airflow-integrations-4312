from datetime import datetime, timedelta, timezone
import hashlib
import json
import rail
from ce_procore_integration.job_totals_sync.utils.constants import SyncType, ContractLevel
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.job_totals_main_dag_id,
        description='Computerease to Procore Job Totals Sync MAIN DAG',
        integration_type='generic',
        company_key=config.instance,
        schedule_interval=config.schedule,
        max_active_runs=config.max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        procore_company_id_template = "{{conn." + \
            config.procore_conn_id + ".extra_dejson.company_id}}"

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_dag_start_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_dag_start_time = rail.PythonOperator(
            task_id='get_dag_start_time',
            python_callable=lambda: datetime.now(timezone.utc).isoformat()
        )

        fetch_job_totals = rail.ComputereaseAPIOperator(
            task_id='fetch_job_totals',
            endpoint='/catalog/job-totals',
            request_method='GET',
            data_handler=lambda response: [
                job for job in response.get('data', [])
                if job.get('phases') and len(job.get('phases', [])) > 0
            ]
        )

        fetch_ce_cost_types = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda response: {
                item['code']: item['reference']
                for item in response.get('data', [])
                if item.get('code') and item.get('reference')
            }
        )

        def parse_direct_cost_data(job, ce_cost_type_mapper):
            items = []
            for phase in job.get('phases', []):
                phase_code = phase.get('phase_code', '')
                for category in phase.get('categories', []):
                    category_code = category.get('category_code', '')
                    for costtype in category.get('costtypes', []):
                        costtype_code = costtype.get('costtype_code', '')
                        if costtype_code.lower() == 'committed':
                            continue
                        cost = float(costtype.get('cost', '0.00'))
                        committed_billed = float(costtype.get('committed_billed', '0.00'))
                        direct_cost = cost - committed_billed
                        costtype_reference = ce_cost_type_mapper.get(costtype_code, '')
                        items.append({
                            'phase_code': phase_code,
                            'category_code': category_code,
                            'costtype_code': costtype_code,
                            'costtype_reference': costtype_reference,
                            'amount': str(round(direct_cost, 2))
                        })
            return items

        def parse_budget_data(job, ce_cost_type_mapper):
            budget_line_items = []

            for phase in job.get('phases', []):
                phase_code = phase.get('phase_code', '')
                for category in phase.get('categories', []):
                    category_code = category.get('category_code', '')
                    for costtype in category.get('costtypes', []):
                        costtype_code = costtype.get('costtype_code', '')

                        # Skip items with committed cost types
                        if costtype_code.lower() == 'committed':
                            continue

                        orig_est_cost = costtype.get('orig_est_cost', '0.00')
                        orig_est_hours = costtype.get(
                            'orig_est_hours', '0.00000')
                        costtype_reference = ce_cost_type_mapper.get(
                            costtype_code, '')

                        budget_line_items.append({
                            'phase_code': phase_code,
                            'category_code': category_code,
                            'costtype_code': costtype_code,
                            'costtype_reference': costtype_reference,
                            'budget_amount': orig_est_cost,
                            'budget_hours': orig_est_hours,
                        })

            return budget_line_items

        def parse_contract_data(job):
            contract_line_items = []

            for phase in job.get('phases', []):
                phase_code = phase.get('phase_code', '').strip()

                # Phase-level contract amounts
                phase_contract_amount = str(
                    phase.get('original_contract_amount', '0.00'))
                if float(phase_contract_amount) != 0:
                    contract_line_items.append({
                        'phase_code': phase_code,
                        'category_code': '',
                        'contract_amount': phase_contract_amount,
                        'level': ContractLevel.PHASE
                    })

                # Category-level contract amounts
                for category in phase.get('categories', []):
                    category_code = category.get('category_code', '').strip()
                    category_contract_amount = str(
                        category.get('original_contract_amount', '0.00'))
                    if float(category_contract_amount) != 0:
                        contract_line_items.append({
                            'phase_code': phase_code,
                            'category_code': category_code,
                            'contract_amount': category_contract_amount,
                            'level': ContractLevel.CATEGORY
                        })

            return contract_line_items

        def parse_required_data():
            jobs_data = rail.result('fetch_job_totals')
            ce_cost_type_mapper = rail.result('fetch_ce_cost_types')
            jobs_with_data = []

            for job in jobs_data:
                job_code = job.get('job_code', '')

                budget_line_items = parse_budget_data(job, ce_cost_type_mapper)
                contract_line_items = parse_contract_data(job)
                direct_cost_line_items = parse_direct_cost_data(job, ce_cost_type_mapper)

                jobs_with_data.append({
                    'job_code': job_code,
                    'budget_line_items': budget_line_items,
                    'contract_line_items': contract_line_items,
                    'direct_cost_line_items': direct_cost_line_items
                })

            return jobs_with_data

        parse_job_totals_data = rail.PythonOperator(
            task_id='parse_job_totals_data',
            python_callable=parse_required_data
        )

        def create_direct_cost_fingerprint(job):
            data = {
                'job_code': job['job_code'],
                'direct_cost_line_items': sorted([
                    {
                        'phase_code': i['phase_code'],
                        'category_code': i['category_code'],
                        'costtype_code': i['costtype_code'],
                        'amount': i['amount']
                    }
                    for i in job['direct_cost_line_items']
                ], key=lambda x: f"{x['phase_code']}-{x['category_code']}-{x['costtype_code']}")
            }
            return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

        def create_budget_fingerprint(job):
            budget_data = {
                'job_code': job['job_code'],
                'budget_line_items': sorted([
                    {
                        'phase_code': item['phase_code'],
                        'category_code': item['category_code'],
                        'costtype_code': item['costtype_code'],
                        'budget_amount': item['budget_amount'],
                        'budget_hours': item['budget_hours']
                    }
                    for item in job['budget_line_items']
                ], key=lambda x: f"{x['phase_code']}-{x['category_code']}-{x['costtype_code']}")
            }

            return hashlib.md5(
                json.dumps(budget_data, sort_keys=True).encode()
            ).hexdigest()

        def create_contract_fingerprint(job):
            contract_data = {
                'job_code': job['job_code'],
                'contract_line_items': sorted([
                    {
                        'phase_code': item['phase_code'],
                        'category_code': item['category_code'],
                        'contract_amount': item['contract_amount'],
                        'level': item['level']
                    }
                    for item in job['contract_line_items']
                ], key=lambda x: f"{x['phase_code']}-{x['category_code']}")
            }
            return hashlib.md5(json.dumps(contract_data, sort_keys=True).encode()).hexdigest()

        def create_job_fingerprints():

            current_jobs = rail.result('parse_job_totals_data')
            fingerprints = []

            for job in current_jobs:
                budget_fingerprint = create_budget_fingerprint(job)
                contract_fingerprint = create_contract_fingerprint(job)
                direct_cost_fingerprint = create_direct_cost_fingerprint(job)

                fingerprints.append({
                    'job_code': job['job_code'],
                    'fingerprint': budget_fingerprint,
                    'contract_fingerprint': contract_fingerprint,
                    'budget_line_items': json.dumps(job['budget_line_items']),
                    'direct_cost_fingerprint': direct_cost_fingerprint,
                    'direct_cost_line_items': json.dumps(job['direct_cost_line_items'])
                })

            return fingerprints

        generate_jobtotals_fingerprints = rail.PythonOperator(
            task_id='generate_jobtotals_fingerprints',
            python_callable=create_job_fingerprints
        )

        search_fingerprint_file_in_s3 = rail.S3ListKeysOperator(
            task_id='search_fingerprint_file_in_s3',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            prefix=config.s3_fingerprints_prefix
        )

        if_previous_fingerprints_found = rail.IfOperator(
            task_id='if_previous_fingerprints_found',
            test='{{ result("search_fingerprint_file_in_s3") | length > 0 and "' +
            config.s3_fingerprints_key +
            '" in result("search_fingerprint_file_in_s3") }}',
            yes_task='download_previous_fingerprints',
            no_task='get_delta_jobs'
        )

        download_previous_fingerprints = rail.S3DownloadFileOperator(
            task_id='download_previous_fingerprints',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_fingerprints_key
        )

        load_previous_fingerprints = rail.LoadCSVFileOperator(
            task_id='load_previous_fingerprints',
            document="{{ result('download_previous_fingerprints') }}"
        )

        def get_force_retry_jobs(dag_run):
            force_retry_jobs = dag_run.conf.get('force_retry_jobs', [])
            if not force_retry_jobs:
                force_retry_jobs = []
            if isinstance(force_retry_jobs, str):
                force_retry_jobs = [force_retry_jobs]
            return force_retry_jobs

        def get_previous_fingerprints_dict():
            previous_fingerprints_exist = len(rail.result(
                'search_fingerprint_file_in_s3')) > 0 and config.s3_fingerprints_key in rail.result('search_fingerprint_file_in_s3')
            if previous_fingerprints_exist:
                previous_fingerprints = rail.load_all_records(
                    rail.result('load_previous_fingerprints'))
                return {fp['job_code']: fp for fp in previous_fingerprints}
            return {}

        def extract_base_fingerprint(fingerprint):
            if '_FAILED_' in fingerprint:
                return fingerprint.split('_FAILED_')[0]
            return fingerprint

        def is_retry_time_reached(previous_fp):
            # Return True if retry time has been reached, False otherwise
            try:
                failure_parts = previous_fp.split(
                    '_FAILED_')[1].split('_RETRYAFTER_')
                if len(failure_parts) > 1:
                    retry_time = datetime.fromisoformat(failure_parts[1])
                    current_time = datetime.now(timezone.utc)
                    retry_buffer = timedelta(
                        minutes=config.retry_buffer_minutes)
                    # Retry if time has been reached
                    return current_time >= (retry_time - retry_buffer)
            except Exception:
                # If we can't parse the retry time, allow retry
                return True

        def _merge_direct_cost_cumulative(previous_cumulative_json, delta_items):
            previous = json.loads(previous_cumulative_json) if previous_cumulative_json else []
            cum_map = {
                f"{i['phase_code']}-{i['category_code']}-{i['costtype_code']}": {**i}
                for i in previous
            }
            for item in delta_items:
                key = f"{item['phase_code']}-{item['category_code']}-{item['costtype_code']}"
                if key in cum_map:
                    cum_map[key] = {
                        **item,
                        'amount': str(round(float(cum_map[key]['amount']) + float(item['amount']), 2))
                    }
                else:
                    cum_map[key] = {**item}
            return list(cum_map.values())

        def _compute_direct_cost_delta_items(current_items, previous_items_json):
            previous_items = json.loads(previous_items_json) if previous_items_json else []
            prev_map = {
                f"{i['phase_code']}-{i['category_code']}-{i['costtype_code']}": float(i['amount'])
                for i in previous_items
            }
            delta_items = []
            for item in current_items:
                key = f"{item['phase_code']}-{item['category_code']}-{item['costtype_code']}"
                delta = float(item['amount']) - prev_map.get(key, 0.0)
                if delta != 0:
                    delta_items.append({**item, 'amount': str(round(delta, 2))})
            return delta_items
        

        def _add_removed_budget_items(job, job_code, previous_fingerprints):
            # Add removed budget items with zero values for proper cleanup
            previous_items_json = rail.find_first_by_attr_and_get_attr(
                previous_fingerprints, 'job_code', job_code, 'budget_line_items', '[]')
            previous_items = json.loads(
                previous_items_json) if previous_items_json else []
            current_items = job['budget_line_items']

            # Create a set of current item keys for comparison
            current_item_keys = {
                f"{item['phase_code']}-{item['category_code']}-{item['costtype_code']}"
                for item in current_items
            }
            # Find removed items (present in previous but not in current)
            for prev_item in previous_items:
                item_key = f"{prev_item['phase_code']}-{prev_item['category_code']}-{prev_item['costtype_code']}"
                if item_key not in current_item_keys:
                    # Add removed item with zero values
                    job['budget_line_items'].append({
                        'phase_code': prev_item['phase_code'],
                        'category_code': prev_item['category_code'],
                        'costtype_code': prev_item['costtype_code'],
                        'costtype_reference': prev_item.get('costtype_reference', ''),
                        'budget_amount': '0.00',
                        'budget_hours': '0.00000'
                    })

        def get_delta_jobs_by_type(sync_type, fingerprint_key, dag_run):
            current_jobs = [{
                'job_code': job['job_code'],
                f'{sync_type}_line_items': job[f'{sync_type}_line_items']
            } for job in rail.result('parse_job_totals_data')]
            current_fingerprints = rail.result(
                'generate_jobtotals_fingerprints')
            previous_fingerprints_exist = len(rail.result(
                'search_fingerprint_file_in_s3')) > 0 and config.s3_fingerprints_key in rail.result('search_fingerprint_file_in_s3')
            jobs_to_force_retry = get_force_retry_jobs(dag_run)
            if previous_fingerprints_exist:  # pylint: disable=too-many-nested-blocks
                previous_fingerprints = rail.load_all_records(
                    rail.result('load_previous_fingerprints'))
                previous_fp_dict = {fp['job_code']: fp.get(fingerprint_key, '')
                                    for fp in previous_fingerprints}
                current_fp_dict = {fp['job_code']: fp.get(fingerprint_key, '')
                                   for fp in current_fingerprints}
                if sync_type == SyncType.DIRECT_COST:
                    previous_dc_items_dict = {
                        fp['job_code']: fp.get('direct_cost_line_items', '[]')
                        for fp in previous_fingerprints
                    }
                delta_jobs = []
                for job in current_jobs:
                    job_code = job['job_code']
                    current_fp = current_fp_dict.get(job_code)
                    previous_fp = previous_fp_dict.get(job_code)
                    # If job existed before and fingerprint changed
                    if previous_fp and current_fp != previous_fp:
                        is_data_changed = current_fp != extract_base_fingerprint(
                            previous_fp)
                        is_retry_requested = job_code in jobs_to_force_retry
                        data_changed_or_retry_requested = is_data_changed or is_retry_requested
                        if '_FAILED_' in previous_fp:
                            if data_changed_or_retry_requested:
                                job['reset_retry_count'] = True
                            else:
                                # Check if retry time has been reached
                                if not is_retry_time_reached(previous_fp):
                                    continue
                        # Handle previous items for delta detection (budget-specific logic)
                        if sync_type == SyncType.BUDGET:
                            _add_removed_budget_items(
                                job, job_code, previous_fingerprints)
                        if sync_type == SyncType.DIRECT_COST:
                            prev_json = previous_dc_items_dict.get(job_code, '[]')
                            job['direct_cost_line_items'] = _compute_direct_cost_delta_items(
                                job['direct_cost_line_items'], prev_json)
                            job['direct_cost_fingerprint'] = current_fp_dict.get(job_code, '')

                        delta_jobs.append(job)
                    # Include new jobs
                    elif previous_fp is None:
                        if sync_type == SyncType.DIRECT_COST:
                            job['direct_cost_line_items'] = _compute_direct_cost_delta_items(
                                job['direct_cost_line_items'], '[]')
                            job['direct_cost_fingerprint'] = current_fp_dict.get(job_code, '')
                        delta_jobs.append(job)

                return delta_jobs
            if sync_type == SyncType.DIRECT_COST:
                current_dc_fp_dict = {fp['job_code']: fp.get('direct_cost_fingerprint', '')
                                      for fp in current_fingerprints}
                for job in current_jobs:
                    job['direct_cost_line_items'] = _compute_direct_cost_delta_items(
                        job['direct_cost_line_items'], '[]')
                    job['direct_cost_fingerprint'] = current_dc_fp_dict.get(job['job_code'], '')
            return current_jobs

        def get_all_delta_jobs(dag_run):
            budget_deltas = get_delta_jobs_by_type(
                SyncType.BUDGET, 'fingerprint', dag_run)
            contract_deltas = get_delta_jobs_by_type(
                SyncType.CONTRACT, 'contract_fingerprint', dag_run)
            direct_cost_deltas = get_delta_jobs_by_type(
                SyncType.DIRECT_COST, 'direct_cost_fingerprint', dag_run)

            return {
                'budget_delta_jobs': budget_deltas,
                'contract_delta_jobs': contract_deltas,
                'direct_cost_delta_jobs': direct_cost_deltas
            }

        get_delta_jobs = rail.PythonOperator(
            task_id='get_delta_jobs',
            python_callable=get_all_delta_jobs
        )

        check_if_delta_jobs_found = rail.IfOperator(
            task_id='check_if_delta_jobs_found',
            test=lambda: (
                len(rail.result("get_delta_jobs")["budget_delta_jobs"]) > 0 or
                len(rail.result("get_delta_jobs")["contract_delta_jobs"]) > 0 or
                len(rail.result("get_delta_jobs")["direct_cost_delta_jobs"]) > 0
            ),
            yes_task='fetch_wbs_segments',
            no_task='log_to_sumo'
        )

        fetch_wbs_segments = rail.ProcoreApiOperator(
            task_id='fetch_wbs_segments',
            endpoint=f'/companies/{procore_company_id_template}/work_breakdown_structure/segments',
            method='GET'
        )

        get_cost_code_segment_id = rail.PythonOperator(
            task_id='get_cost_code_segment_id',
            python_callable=lambda: next(
                (
                    segment['id'] for segment in rail.result('fetch_wbs_segments')
                    if segment['name'] == config.cost_code_segment_name
                    and segment['type'] == config.cost_code_segment_type
                ),
                None
            )
        )

        get_cost_type_segment_id = rail.PythonOperator(
            task_id='get_cost_type_segment_id',
            python_callable=lambda: next(
                (
                    segment['id'] for segment in rail.result('fetch_wbs_segments')
                    if segment['name'] == config.cost_type_segment_name
                    and segment['type'] == config.cost_type_segment_type
                ),
                None
            )
        )

        fetch_cost_type_segment_items = rail.ProcoreApiOperator(
            task_id='fetch_cost_type_segment_items',
            endpoint=lambda: f'/companies/{rail.render_template(procore_company_id_template)}/work_breakdown_structure/segments/{rail.result("get_cost_type_segment_id")}/segment_items',  # pylint: disable=line-too-long
            method='GET',
            data_handler=lambda items: {
                item.get('code', ''): item.get('id') for item in items if item.get('code')
            }
        )

        check_if_revenue_cost_type_exists = rail.IfOperator(
            task_id='check_if_revenue_cost_type_exists',
            test=lambda: rail.result("fetch_cost_type_segment_items").get(
                config.revenue_cost_type) is not None,
            yes_task='update_revenue_cost_type',
            no_task='create_revenue_cost_type'
        )

        create_revenue_cost_type = rail.ProcoreApiOperator(
            task_id='create_revenue_cost_type',
            endpoint=lambda: f'/companies/{rail.render_template(procore_company_id_template)}/work_breakdown_structure/segments/{rail.result("get_cost_type_segment_id")}/segment_items',
            method='POST',
            data={
                'code': config.revenue_cost_type,
                'name': config.revenue_cost_type_name
            }
        )

        update_revenue_cost_type = rail.ProcoreApiOperator(
            task_id='update_revenue_cost_type',
            endpoint=lambda: f'/companies/{rail.render_template(procore_company_id_template)}/work_breakdown_structure/segments/{rail.result("get_cost_type_segment_id")}/segment_items/{rail.result("fetch_cost_type_segment_items").get(config.revenue_cost_type)}',
            method='PATCH',
            data={
                'code': config.revenue_cost_type,
                'name': config.revenue_cost_type_name
            }
        )

        trigger_budget_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_budget_child_dag',
            items=lambda: rail.result('get_delta_jobs')['budget_delta_jobs'],
            trigger_dag_id=config.job_totals_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'job_data': item,
                'procore_company_id': rail.render_template(procore_company_id_template),
                'cost_code_segment_id': rail.result('get_cost_code_segment_id'),
                'cost_type_segment_id': rail.result('get_cost_type_segment_id'),
                'cost_type_segment_items': rail.result('fetch_cost_type_segment_items')
            }
        )

        wait_for_budget_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_budget_completion',
            dag_runs='{{ result("trigger_budget_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        trigger_prime_contract_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_prime_contract_child_dag',
            items=lambda: rail.result('get_delta_jobs')['contract_delta_jobs'],
            trigger_dag_id=config.contract_line_items_sync_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'job_code': item['job_code'],
                'contract_line_items': item['contract_line_items'],
                'procore_company_id': rail.render_template(procore_company_id_template),
                'reset_retry_count': item.get('reset_retry_count', False)
            }
        )

        wait_for_contract_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_contract_completion',
            dag_runs='{{ result("trigger_prime_contract_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        trigger_direct_cost_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_direct_cost_child_dag',
            items=lambda: [
                job for job in rail.result('get_delta_jobs')['direct_cost_delta_jobs']
                if job.get('direct_cost_line_items')
            ],
            trigger_dag_id=config.direct_cost_sync_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'job_data': item,
                'procore_company_id': rail.render_template(procore_company_id_template),
                'cost_code_segment_id': rail.result('get_cost_code_segment_id'),
                'cost_type_segment_items': rail.result('fetch_cost_type_segment_items')
            }
        )

        wait_for_direct_cost_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_direct_cost_completion',
            dag_runs='{{ result("trigger_direct_cost_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        def calculate_failure_fingerprint(base_fingerprint, previous_stored_fingerprint, dag_start_time, reset_retry_count=False):
            try:
                # Extract failure count from previous stored fingerprint
                if '_FAILED_' in previous_stored_fingerprint and not reset_retry_count:
                    failure_parts = previous_stored_fingerprint.split(
                        '_FAILED_')[1].split('_RETRYAFTER_')
                    count = int(failure_parts[0]) + 1
                else:
                    # First failure or reset due to data change/force retry
                    count = 1

                retry_delay_array = config.retry_delays_hours
                if count <= len(retry_delay_array):
                    retry_delay = timedelta(hours=retry_delay_array[count - 1])
                else:
                    # Use last configured delay for attempts beyond the array
                    retry_delay = timedelta(hours=retry_delay_array[-1])

                next_retry_time = dag_start_time + retry_delay
                return f"{base_fingerprint}_FAILED_{count}_RETRYAFTER_{next_retry_time.isoformat()}"

            except Exception:
                # Default fallback: treat as first failure with immediate retry
                return f"{base_fingerprint}_FAILED_1_RETRYAFTER_{dag_start_time.isoformat()}"

        def categorize_failed_jobs_by_sync_type():
            if rail.result('search_logs', 'length') == 0:
                return {'budget': set(), 'contract': set(), 'direct_cost': set()}, {'budget': set(), 'contract': set(), 'direct_cost': set()}

            error_logs = rail.load_all_records(rail.result('search_logs'))
            failed_jobs_by_type = {'budget': set(), 'contract': set(), 'direct_cost': set()}
            reset_retry_jobs_by_type = {'budget': set(), 'contract': set(), 'direct_cost': set()}

            for log_entry in error_logs:
                job_code = log_entry.get('properties', {}).get('entity_code')
                sync_type = log_entry.get('properties', {}).get(
                    'sync_type', SyncType.BUDGET)

                if job_code:
                    failed_jobs_by_type[sync_type].add(job_code)
                    if log_entry.get('properties', {}).get('reset_retry_count', False):
                        reset_retry_jobs_by_type[sync_type].add(job_code)

            return failed_jobs_by_type, reset_retry_jobs_by_type

        def adjust_fingerprint_by_type(fp_copy, job_code, sync_type, failed_jobs, reset_retry_jobs, previous_fp_dict, dag_start_time, not_processed_this_run):
            fingerprint_key_map = {
                SyncType.BUDGET: 'fingerprint',
                SyncType.CONTRACT: 'contract_fingerprint',
                SyncType.DIRECT_COST: 'direct_cost_fingerprint'
            }
            fingerprint_key = fingerprint_key_map[sync_type]
            previous_fp = previous_fp_dict.get(
                job_code, {}).get(fingerprint_key, '')
            current_fp = fp_copy.get(fingerprint_key, '')
            if job_code in failed_jobs[sync_type]:
                reset_retry_count = job_code in reset_retry_jobs[sync_type]
                fp_copy[fingerprint_key] = calculate_failure_fingerprint(
                    current_fp,
                    previous_fp,
                    dag_start_time,
                    reset_retry_count
                )
            elif not_processed_this_run: # If Job was not processed this run and base fingerprint unchanged then retain previous fingerprint
                if previous_fp and (current_fp in previous_fp):
                    fp_copy[fingerprint_key] = previous_fp

        def adjust_fingerprints_for_failed_jobs():
            """
            Adjusts fingerprints for jobs that encountered exception/error.
            Updates direct_cost_line_items as the cumulative Procore total (only advances on success).
            """
            all_fingerprints = rail.result('generate_jobtotals_fingerprints')
            previous_fp_dict = get_previous_fingerprints_dict()
            budget_deltas = {job['job_code'] for job in rail.result('get_delta_jobs')['budget_delta_jobs']}
            contract_deltas = {job['job_code'] for job in rail.result('get_delta_jobs')['contract_delta_jobs']}
            direct_cost_deltas = {job['job_code'] for job in rail.result('get_delta_jobs')['direct_cost_delta_jobs']}

            direct_cost_delta_map = {
                job['job_code']: job['direct_cost_line_items']
                for job in rail.result('get_delta_jobs')['direct_cost_delta_jobs']
            }

            failed_jobs_by_type, reset_retry_jobs_by_type = categorize_failed_jobs_by_sync_type()

            adjusted_fingerprints = []
            dag_start_time = datetime.fromisoformat(
                rail.result('get_dag_start_time'))

            for fp in all_fingerprints:
                fp_copy = fp.copy()
                job_code = fp['job_code']
                adjust_fingerprint_by_type(fp_copy, job_code, SyncType.BUDGET, failed_jobs_by_type,
                                           reset_retry_jobs_by_type, previous_fp_dict, dag_start_time,
                                           not_processed_this_run=job_code not in budget_deltas)
                adjust_fingerprint_by_type(fp_copy, job_code, SyncType.CONTRACT, failed_jobs_by_type,
                                           reset_retry_jobs_by_type, previous_fp_dict, dag_start_time,
                                           not_processed_this_run=job_code not in contract_deltas)
                adjust_fingerprint_by_type(fp_copy, job_code, SyncType.DIRECT_COST, failed_jobs_by_type,
                                           reset_retry_jobs_by_type, previous_fp_dict, dag_start_time,
                                           not_processed_this_run=job_code not in direct_cost_deltas)

                prev_cumulative_json = previous_fp_dict.get(job_code, {}).get('direct_cost_line_items', '[]')
                if job_code in direct_cost_deltas and job_code not in failed_jobs_by_type[SyncType.DIRECT_COST]:
                    delta_items = direct_cost_delta_map.get(job_code, [])
                    fp_copy['direct_cost_line_items'] = json.dumps(
                        _merge_direct_cost_cumulative(prev_cumulative_json, delta_items)
                    )
                else:
                    fp_copy['direct_cost_line_items'] = prev_cumulative_json if prev_cumulative_json else '[]'

                adjusted_fingerprints.append(fp_copy)

            return adjusted_fingerprints

        adjust_fingerprints = rail.PythonOperator(
            task_id='adjust_fingerprints',
            python_callable=adjust_fingerprints_for_failed_jobs
        )

        write_current_fingerprints_csv = rail.WriteCSVFileOperator(
            task_id='write_current_fingerprints_csv',
            source="{{ result('adjust_fingerprints') | to_json }}",
            header=['job_code', 'fingerprint', 'budget_line_items',
                    'contract_fingerprint', 'direct_cost_fingerprint', 'direct_cost_line_items'],
            row=["{{ item.job_code }}", "{{ item.fingerprint }}", "{{ item.budget_line_items }}",
                 "{{ item.contract_fingerprint }}", "{{ item.direct_cost_fingerprint }}", "{{ item.direct_cost_line_items }}"]
        )

        upload_current_fingerprints_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_current_fingerprints_to_s3',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('write_current_fingerprints_csv') }}",
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_fingerprints_key,
            replace=True
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test='{{ result("search_logs", "length") > 0 }}',
            yes_task='write_logs_into_csv',
            no_task='log_to_sumo'
        )

        write_logs_into_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_into_csv',
            source='{{ result("search_logs") }}',
            header=['Job Code', 'Reason', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('entity_code','') }}",
                "{{ item.properties | attr_or_default('error_message','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_JobTotalsSyncLogs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_notification_with_logs = rail.EmailOperator(
            task_id='send_notification_with_logs',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Job Totals Sync completed with errors - {{ current_time() }}',
            html_content='templates/job_totals_sync_logs_notification.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_dag_start_time >> fetch_job_totals >> fetch_ce_cost_types >> parse_job_totals_data
        parse_job_totals_data >> generate_jobtotals_fingerprints >> search_fingerprint_file_in_s3 >> if_previous_fingerprints_found
        if_previous_fingerprints_found >> rail.Label(
            'Yes') >> download_previous_fingerprints >> load_previous_fingerprints >> get_delta_jobs
        if_previous_fingerprints_found >> rail.Label('No') >> get_delta_jobs

        get_delta_jobs >> check_if_delta_jobs_found
        check_if_delta_jobs_found >> rail.Label('No') >> log_to_sumo
        check_if_delta_jobs_found >> rail.Label('Yes') >> fetch_wbs_segments >> get_cost_code_segment_id >> get_cost_type_segment_id >> fetch_cost_type_segment_items >> check_if_revenue_cost_type_exists

        check_if_revenue_cost_type_exists >> rail.Label('Yes') >> update_revenue_cost_type >> trigger_budget_child_dag
        check_if_revenue_cost_type_exists >> rail.Label('No') >> create_revenue_cost_type >> trigger_budget_child_dag >> wait_for_budget_completion

        wait_for_budget_completion >> trigger_prime_contract_child_dag >> wait_for_contract_completion >> trigger_direct_cost_child_dag
        trigger_direct_cost_child_dag >> wait_for_direct_cost_completion >> search_logs >> adjust_fingerprints >> write_current_fingerprints_csv
        write_current_fingerprints_csv >> upload_current_fingerprints_to_s3 >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_logs_download_link >> send_notification_with_logs >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo
        batch_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
