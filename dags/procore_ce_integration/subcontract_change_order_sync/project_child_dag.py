import rail
from datetime import timedelta
from procore_ce_integration.subcontract_change_order_sync.utils.constants import SkipReason, SyncStatus
from procore_ce_integration.job_structure_sync.utils.constants import WBSType
from procore_ce_integration.initial_setup_sync.shared_utils import normalize_ce_identifier


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.project_child_dag_id,
        description='Procore to ComputerEase Subcontract Change Order Sync - Project Child DAG',
        max_active_runs=config.max_active_runs_project_child,
        integration_type='generic',
        company_key=config.instance,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_project_job_code',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def extract_project_number(response, dag_run):
            origin_id = rail.find_first_by_attr_and_get_attr(
                response, 'id', dag_run.conf['project_id'], 'origin_id', '')
            if origin_id and origin_id.startswith('CE_'):
                origin_id = origin_id[3:]
            return normalize_ce_identifier(origin_id)

        fetch_project_job_code = rail.ProcoreApiOperator(
            task_id='fetch_project_job_code',
            endpoint='/projects',
            method='GET',
            query_params={
                'company_id': '{{ dag_run.conf.company_id }}',
                'filters[id]': '{{ dag_run.conf.project_id }}'
            },
            data_handler=extract_project_number
        )

        has_project_origin_id = rail.IfOperator(
            task_id='has_project_origin_id',
            test="{{ result('fetch_project_job_code') | is_truthy }}",
            yes_task='identify_wbs_type_from_job',
            no_task='log_skipped_project'
        )

        def identify_wbs_type(response):
            job_wbs_type = (response.get('data') or [{}])[0].get('wbs_type', '')
            return job_wbs_type

        identify_wbs_type_from_job = rail.ComputereaseAPIOperator(
            task_id='identify_wbs_type_from_job',
            endpoint='/catalog/job',
            request_method='GET',
            query_params={
                'code': "{{ result('fetch_project_job_code') }}"
            },
            data_handler=identify_wbs_type
        )

        has_wbs_type = rail.IfOperator(
            task_id='has_wbs_type',
            test="{{ result('identify_wbs_type_from_job') | is_truthy }}",
            yes_task='is_wbs_type_TM',
            no_task='log_skipped_project_no_wbs_type'
        )

        log_skipped_project_no_wbs_type = rail.WriteLogOperator(
            task_id='log_skipped_project_no_wbs_type',
            message='Subcontract Change Order Sync skipped - Job not found in ComputerEase — sync job first before syncing subcontract change orders',
            severity='Error/Exception',
            items=lambda dag_run: dag_run.conf['cop_ids'],
            properties={
                'cop_id': '{{ item }}',
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': "{{ result('fetch_project_job_code') }}",
                'reason': SkipReason.WBS_TYPE_EMPTY,
                'message': 'Subcontract Change Order Sync skipped - Job not found in ComputerEase — sync job first before syncing subcontract change orders',
                'sync_status': SyncStatus.SKIPPED
            }
        )

        is_wbs_type_TM = rail.IfOperator(
            task_id='is_wbs_type_TM',
            test="{{ result('identify_wbs_type_from_job') == '%s' }}" % WBSType.TIME_MATERIAL,
            yes_task='log_skipped_project_wbs_type',
            no_task='fetch_project_cops'
        )

        log_skipped_project_wbs_type = rail.WriteLogOperator(
            task_id='log_skipped_project_wbs_type',
            message='Subcontract Change Order Sync skipped - Job WBS Type is Time and Materials',
            severity='Error/Exception',
            items=lambda dag_run: dag_run.conf['cop_ids'],
            properties={
                'cop_id': '{{ item }}',
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': "{{ result('fetch_project_job_code') }}",
                'reason': SkipReason.WBS_TYPE_TM,
                'message': 'Job WBS Type is Time and Materials',
                'sync_status': SyncStatus.SKIPPED
            }
        )

        def filter_valid_cops(response, dag_run):
            project_id = dag_run.conf['project_id']
            valid_cops = []
            skipped_cops = []

            for cop in response:
                cop_id = cop['id']
                status = cop['status'].lower()

                if status not in config.syncable_cop_statuses:
                    skipped_cops.append({
                        'cop_id': cop_id,
                        'project_id': project_id,
                        'reason': SkipReason.INVALID_STATUS,
                        'message': f"Subcontract Change Order {cop_id} has status '{status}' which should not be synced",
                        'sync_status': SyncStatus.SKIPPED
                    })
                    continue

                grand_total = float(cop.get('grand_total') or 0)
                if not config.allow_zero_amounts and grand_total == 0:
                    skipped_cops.append({
                        'cop_id': cop_id,
                        'project_id': project_id,
                        'reason': SkipReason.ALL_AMOUNTS_ZERO,
                        'message': f"Subcontract Change Order {cop_id} has zero grand_total",
                        'sync_status': SyncStatus.SKIPPED
                    })
                    continue

                cop['project_id'] = project_id
                cop['job_code'] = rail.result('fetch_project_job_code')
                cop['wbs_type'] = rail.result('identify_wbs_type_from_job')
                valid_cops.append(cop)

            return {
                'valid_cops': valid_cops,
                'skipped_cops': skipped_cops
            }

        fetch_project_cops = rail.ProcoreApiOperator(
            task_id='fetch_project_cops',
            endpoint=lambda dag_run: f"/change_order_packages?filters[id]={dag_run.conf['cop_ids']}",
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data_handler=filter_valid_cops
        )

        log_skipped_project = rail.WriteLogOperator(
            task_id='log_skipped_project',
            message='Subcontract Change Order Sync skipped - Job does not exist in ComputerEase',
            severity='Error/Exception',
            items=lambda dag_run: dag_run.conf['cop_ids'],
            properties={
                'cop_id': '{{ item }}',
                'project_id': '{{ dag_run.conf.project_id }}',
                'job_code': 'NA',
                'reason': SkipReason.JOB_NOT_PRESENT,
                'message': 'Job does not exist in ComputerEase',
                'sync_status': SyncStatus.SKIPPED
            }
        )

        has_skipped_cops = rail.IfOperator(
            task_id='has_skipped_cops',
            test="{{ result('fetch_project_cops').skipped_cops | length > 0 }}",
            yes_task='log_skipped_cops',
            no_task='valid_cops'
        )

        log_skipped_cops = rail.WriteLogOperator(
            task_id='log_skipped_cops',
            message='Subcontract Change Order Sync skipped due to validation',
            severity='Error/Exception',
            items=lambda: rail.result('fetch_project_cops')['skipped_cops'],
            properties={
                'cop_id': '{{ item.cop_id }}',
                'project_id': '{{ item.project_id }}',
                'job_code': "{{ result('fetch_project_job_code') }}",
                'reason': '{{ item.reason }}',
                'message': '{{ item.message }}',
                'sync_status': '{{ item.sync_status }}'
            }
        )

        valid_cops = rail.PythonOperator(
            task_id='valid_cops',
            python_callable=lambda: rail.result(
                'fetch_project_cops')['valid_cops']
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        batch_task >> fetch_project_job_code >> has_project_origin_id

        has_project_origin_id >> rail.Label(
            'Yes') >> identify_wbs_type_from_job >> has_wbs_type

        has_wbs_type >> rail.Label(
            'Yes') >> is_wbs_type_TM
        has_wbs_type >> rail.Label(
            'No') >> log_skipped_project_no_wbs_type >> finish

        is_wbs_type_TM >> rail.Label(
            'Yes') >> log_skipped_project_wbs_type >> finish

        is_wbs_type_TM >> rail.Label(
            'No') >> fetch_project_cops >> has_skipped_cops

        has_skipped_cops >> rail.Label(
            'Yes') >> log_skipped_cops >> valid_cops
        has_skipped_cops >> rail.Label(
            'No') >> valid_cops

        valid_cops >> finish

        has_project_origin_id >> rail.Label(
            'No') >> log_skipped_project >> finish

        batch_task >> finish

    return dag


rail.for_each_instance(create_dag_instance)
