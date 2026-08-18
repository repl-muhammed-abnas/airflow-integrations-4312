from datetime import timedelta
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from ce_procore_integration.job_structure_sync.utils.job_parser import parse_job_data, parse_phase_data, parse_category_data
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.job_main_dag_id,
        description='Computerease to Procore Job Sync MAIN DAG',
        schedule_interval=timedelta(minutes=config.job_sync_interval_minutes),
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.job_last_sync_time_var,
                date_format=config.ce_time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            ),
        )

        def identify_query_params_func(dag_run):
            """
            Determines query parameters based on the sync mode:
            1. Single job reconciliation - if job_code provided in conf
            2. Initial sync / Normal delta sync - uses last sync time
            """
            job_code = dag_run.conf.get('job_code') if dag_run.conf else None

            if job_code:
                # Single job reconciliation mode
                return {
                    'is_single_job_reconcile': True,
                    'is_initial_sync': False,
                    'job_query_params': {
                        'code': job_code
                    },
                    'phase_query_params': {
                        'job_code': job_code
                    },
                    'category_query_params': {
                        'job_code': job_code
                    }
                }
            # Normal mode - use last sync time from previous task
            last_sync_time = rail.result('get_last_sync_time')[
                'last_synctime']

            # Use last sync time for delta queries
            query_filter = {
                'gt~updated_at': last_sync_time
            }
            
            is_initial_sync = last_sync_time == config.initial_sync_time

            return {
                'is_single_job_reconcile': False,
                'is_initial_sync': is_initial_sync,
                'job_query_params': {
                    **query_filter,
                    'status': 'active'
                } if is_initial_sync else query_filter,
                'phase_query_params': query_filter,
                'category_query_params': query_filter
            }

        identify_query_params = rail.PythonOperator(
            task_id='identify_query_params',
            python_callable=identify_query_params_func
        )

        fetch_computerease_jobs = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_jobs',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: rail.result("identify_query_params")[
                "job_query_params"]
        )

        fetch_computerease_phases = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_phases',
            endpoint='/catalog/phase',
            request_method='GET',
            query_params=lambda: rail.result("identify_query_params")[
                "phase_query_params"]
        )

        fetch_computerease_categories = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_categories',
            endpoint='/catalog/category',
            request_method='GET',
            query_params=lambda: rail.result("identify_query_params")[
                "category_query_params"]
        )

        should_skip_set_sync_time = rail.IfOperator(
            task_id='should_skip_set_sync_time',
            test=lambda dag_run: dag_run.conf and dag_run.conf.get(
                'job_code') and rail.result("identify_query_params")["is_single_job_reconcile"],
            yes_task='has_jobs_to_sync',
            no_task='set_last_sync_time'
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.job_last_sync_time_var,
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )
        )

        procore_company_id_template = "{{conn." + \
            config.procore_conn_id + ".extra_dejson.company_id}}"

        def create_sync_batch():
            # Get all delta data
            jobs_data = rail.result('fetch_computerease_jobs').get('data', [])
            phases_data = rail.result(
                'fetch_computerease_phases').get('data', [])
            categories_data = rail.result(
                'fetch_computerease_categories').get('data', [])

            # Get sync mode info from previous task
            is_initial_sync = rail.result('identify_query_params').get('is_initial_sync', False)

            # Create batch items grouped by job_code
            sync_batch = {}

            # 1. Add jobs from direct job updates
            for job in jobs_data:
                job_code = job.get('code')
                if job_code:
                    sync_batch[job_code] = {
                        'job_code': job_code,
                        'job_data': job,
                        'phases': {},
                        'direct_categories': {}
                    }

            # 2. Group phases by their parent job_code
            for phase in phases_data:
                job_code = phase.get('job_code')
                phase_code = phase.get('code')
                if job_code and phase_code:
                    # Skip phases from inactive jobs during initial sync
                    if is_initial_sync and job_code not in sync_batch:
                        continue
                        
                    if job_code not in sync_batch:
                        sync_batch[job_code] = {
                            'job_code': job_code,
                            'job_data': None,
                            'phases': {},
                            'direct_categories': {}
                        }
                    sync_batch[job_code]['phases'][phase_code] = {
                        'phase_code': phase_code,
                        'phase_data': parse_phase_data(phase),
                        'categories': {}
                    }

            # 3. Group categories by their parent job_code and phase_code
            for category in categories_data:
                job_code = category.get('job_code')
                phase_code = category.get('phase_code')
                cat_code = category.get('code')

                if job_code and cat_code:
                    # Skip categories from inactive jobs during initial sync
                    if is_initial_sync and job_code not in sync_batch:
                        continue
                        
                    if job_code not in sync_batch:
                        sync_batch[job_code] = {
                            'job_code': job_code,
                            'job_data': None,
                            'phases': {},
                            'direct_categories': {}
                        }

                    if phase_code:
                        # Category belongs to a phase - ensure parent phase exists
                        if phase_code not in sync_batch[job_code]['phases']:
                            # Create phase entry even if phase itself didn't change
                            sync_batch[job_code]['phases'][phase_code] = {
                                'phase_code': phase_code,
                                'phase_data': None,
                                'categories': {}
                            }
                        sync_batch[job_code]['phases'][phase_code]['categories'][cat_code] = {
                            'category_code': cat_code,
                            'category_data': parse_category_data(category)
                        }
                    else:
                        # Direct category (Job/CAT structure)
                        sync_batch[job_code]['direct_categories'][cat_code] = {
                            'category_code': cat_code,
                            'category_data': parse_category_data(category)
                        }

            return list(sync_batch.values())

        aggregate_jobs = rail.PythonOperator(
            task_id='aggregate_jobs',
            python_callable=create_sync_batch
        )

        has_jobs_to_sync = rail.IfOperator(
            task_id='has_jobs_to_sync',
            test='{{ result("aggregate_jobs") | length > 0 }}',
            yes_task='fetch_ce_udf_definitions',
            no_task='log_to_sumo'
        )

        fetch_ce_udf_definitions = rail.ComputereaseAPIOperator(
            task_id='fetch_ce_udf_definitions',
            endpoint='/catalog/udf-def',
            request_method='GET',
            query_params={
                'table_name': 'jobs'
            },
            data_handler=lambda response: next((udf.get('id') for udf in response.get('data', [])
                                                if udf.get('field_name') == config.project_template_udf_field_name), None)
        )

        fetch_procore_departments = rail.ProcoreApiOperator(
            task_id='fetch_procore_departments',
            endpoint=f'/departments?company_id={procore_company_id_template}',
            method='GET'
        )

        fetch_procore_project_templates = rail.ProcoreApiOperator(
            task_id='fetch_procore_project_templates',
            endpoint=f'/project_templates?company_id={procore_company_id_template}',
            method='GET',
            data_handler=lambda response: {template.get('name'): template.get(
                'id') for template in response if template.get('name') and template.get('id')}
        )

        def create_department_lookup(departments_data):
            # Procore department names should be in format "Code - Name"
            lookup = {}
            for dept in departments_data:
                dept_name = dept.get('name', '')
                dept_id = dept.get('id')
                if dept_id and '-' in dept_name:
                    # Extract code from "Code - Name" format
                    code = dept_name.split('-')[0].strip()
                    if code:
                        lookup[code] = dept_id
            return lookup

        def build_project_lookup(projects):
            if not projects:
                return {'ids': {}, 'duplicate': []}
            by_number = {}
            for project in projects:
                number = project.get('project_number')
                if not number:
                    continue
                by_number.setdefault(number, []).append(project)
            ids = {}
            duplicate = []
            for project_number, group in by_number.items():
                if len(group) == 1:
                    ids[project_number] = group[0]['id']
                else:
                    matched = next(
                        (p for p in group if p.get('origin_id') == f"CE_{project_number}"),
                        None
                    )
                    if matched:
                        ids[project_number] = matched['id']
                    else:
                        duplicate.append(project_number)
            return {'ids': ids, 'duplicate': duplicate}

        fetch_all_procore_projects = rail.ProcoreApiOperator(
            task_id='fetch_all_procore_projects',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=build_project_lookup
        )

        def prepare_batch_items(aggregated_jobs):
            department_lookup = create_department_lookup(
                rail.result('fetch_procore_departments'))
            project_template_udf_id = rail.result('fetch_ce_udf_definitions')
            project_templates_lookup = rail.result(
                'fetch_procore_project_templates')
            project_lookup_result = rail.result('fetch_all_procore_projects') or {}
            project_ids = project_lookup_result.get('ids', {})
            duplicate_project_numbers = set(project_lookup_result.get('duplicate', []))
            jobs = []
            for job in aggregated_jobs:
                job_code = job.get('job_code', '')
                job_data = parse_job_data(
                    job['job_data'], department_lookup, project_template_udf_id) if job['job_data'] else None

                jobs.append({
                    **job,
                    'job_data': job_data,
                    'procore_company_id': rail.render_template(procore_company_id_template),
                    'department_lookup': department_lookup,
                    'project_template_udf_id': project_template_udf_id,
                    'project_templates_lookup': project_templates_lookup,
                    'procore_project_id': project_ids.get(job_code),
                    'had_duplicates_in_procore': job_code in duplicate_project_numbers
                })

            return jobs

        trigger_job_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_job_sync_child_dag',
            items=lambda: prepare_batch_items(rail.result('aggregate_jobs')),
            trigger_dag_id=config.job_child_dag_v2_id or config.job_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: item
        )

        wait_for_job_sync_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_job_sync_completion',
            dag_runs='{{ result("trigger_job_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
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
            header=['WBS Type', 'Code', 'Full Code',
                    'Name', 'Error Message', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('entity_type','') }}",
                "{{ item.properties | attr_or_default('entity_code','') }}",
                "{{ item.properties | attr_or_default('full_code','') }}",
                "{{ item.properties | attr_or_default('entity_name','') }}",
                "{{ item.properties | attr_or_default('error_message','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_JobSyncLogs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_notification_with_logs = rail.EmailOperator(
            task_id='send_notification_with_logs',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Job Structure Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/job_sync_logs_notification.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_last_sync_time >> identify_query_params >> fetch_computerease_jobs >> fetch_computerease_phases >> fetch_computerease_categories
        fetch_computerease_categories >> aggregate_jobs >> should_skip_set_sync_time
        batch_task >> log_to_sumo

        should_skip_set_sync_time >> rail.Label(
            'No') >> set_last_sync_time >> has_jobs_to_sync
        should_skip_set_sync_time >> rail.Label('Yes') >> has_jobs_to_sync

        has_jobs_to_sync >> rail.Label(
            'Yes') >> fetch_ce_udf_definitions >> fetch_procore_departments >> fetch_procore_project_templates
        fetch_procore_project_templates >> fetch_all_procore_projects >> trigger_job_sync_child_dag >> wait_for_job_sync_completion >> search_logs
        has_jobs_to_sync >> rail.Label('No') >> log_to_sumo

        search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_logs_download_link >> send_notification_with_logs >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
