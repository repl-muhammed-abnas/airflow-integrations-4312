from datetime import timedelta
import rail
from ce_procore_integration.job_structure_sync.utils.job_parser import parse_phase_data


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.phases_child_dag_id,
        description='Computerease to Procore phases sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.phase_child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_if_need_phase_fetch',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_need_phase_fetch = rail.IfOperator(
            task_id='check_if_need_phase_fetch',
            test=lambda dag_run: dag_run.conf.get('phase_data') is None,
            yes_task='fetch_phase_data',
            no_task='prepare_phase_data'
        )

        fetch_phase_data = rail.ComputereaseAPIOperator(
            task_id='fetch_phase_data',
            endpoint='/catalog/phase',
            request_method='GET',
            query_params={
                'job_code': '{{ dag_run.conf.job_code }}',
                'code': '{{ dag_run.conf.phase_code }}'
            }
        )

        def get_phase_data_for_sync(dag_run):
            # Use provided phase_data or parse fetched data
            if dag_run.conf.get('phase_data') is not None:
                return dag_run.conf['phase_data']
            phase_raw_data = rail.result(
                'fetch_phase_data').get('data', [])
            if phase_raw_data:
                # Parse the raw data and add the ID from item_id_lookup
                parsed_data = parse_phase_data(phase_raw_data[0])
                if parsed_data:
                    parsed_data['id'] = dag_run.conf.get(
                        'parent_id_lookup', {}).get(dag_run.conf['phase_code'], None)
                return parsed_data
            raise ValueError(
                f"Phase not found in CE: {dag_run.conf['phase_code']}")

        prepare_phase_data = rail.PythonOperator(
            task_id='prepare_phase_data',
            python_callable=get_phase_data_for_sync
        )

        check_if_phase_exists = rail.IfOperator(
            task_id='check_if_phase_exists',
            test=lambda: rail.result(
                'prepare_phase_data').get('id') is not None,
            yes_task='update_phase_to_procore',
            no_task='create_phase_to_procore'
        )

        def get_create_or_update_phase_payload():
            phase_data = rail.result('prepare_phase_data')

            name = phase_data.get('description', '')
            if not name:
                name = phase_data.get('code', '')
            if not name:
                raise ValueError(
                    "Phase must have either description or code for sync")

            return {
                "name": name,
                "code": phase_data.get('code', ''),
                "parent_id": None
            }

        update_phase_to_procore = rail.ProcoreApiOperator(
            task_id='update_phase_to_procore',
            # pylint: disable=line-too-long
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/work_breakdown_structure/segments/{dag_run.conf["cost_code_segment_id"]}/segment_items/{rail.result("prepare_phase_data")["id"]}',
            method='PATCH',
            data=get_create_or_update_phase_payload
        )

        create_phase_to_procore = rail.ProcoreApiOperator(
            task_id='create_phase_to_procore',
            endpoint='/projects/{{ dag_run.conf.procore_project_id }}/work_breakdown_structure/segments/{{ dag_run.conf.cost_code_segment_id }}/segment_items',
            method='POST',
            data=get_create_or_update_phase_payload
        )

        check_if_has_categories = rail.IfOperator(
            task_id='check_if_has_categories',
            test=lambda dag_run: len(dag_run.conf.get('categories', {})) > 0,
            yes_task='trigger_categories_sync_child_dag',
            no_task='catch_error'
        )

        def get_phase_segment_item_id():
            # Get the phase segment item ID from either create or update result
            create_result = rail.result('create_phase_to_procore')
            update_result = rail.result('update_phase_to_procore')
            phase_data = rail.result('prepare_phase_data')

            if create_result and 'id' in create_result:
                return create_result['id']
            if update_result and 'id' in update_result:
                return update_result['id']
            if phase_data and phase_data.get('id'):
                return phase_data['id']
            raise ValueError(
                "Unable to determine phase segment item ID for syncing the categories.")

        def get_conf_for_category_child_dag(dag_run, item):
            phase_segment_item_id = get_phase_segment_item_id()
            return {
                'category_code': item.get('category_code', ''),
                'category_data': {
                    **item.get('category_data', {}),
                    'id': dag_run.conf.get('child_id_lookup', {}).get(
                        f"{phase_segment_item_id}:{item.get('category_code', '')}",
                        None
                    )
                },
                'job_code': dag_run.conf['job_code'],
                'phase_code': dag_run.conf['phase_code'],
                'wbs_type': dag_run.conf['wbs_type'],
                'procore_project_id': dag_run.conf['procore_project_id'],
                'procore_company_id': dag_run.conf['procore_company_id'],
                'cost_code_segment_id': dag_run.conf['cost_code_segment_id'],
                'phase_parent_id': phase_segment_item_id,
                'parent_type': 'phase'
            }

        trigger_categories_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_categories_sync_child_dag',
            items=lambda dag_run: list(
                dag_run.conf.get('categories', {}).values()),
            trigger_dag_id=config.category_child_dag_id,
            conf=lambda dag_run, item: get_conf_for_category_child_dag(dag_run, item)
        )

        wait_for_categories_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_categories_completion',
            dag_runs='{{ result("trigger_categories_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PHASE',
                'entity_code': dag_run.conf.get('phase_code', ''),
                'full_code': f"{dag_run.conf.get('job_code', '')}.{dag_run.conf.get('phase_code', '')}",
                'entity_name': rail.result('prepare_phase_data').get('description', '') if rail.result('prepare_phase_data') else '',
                'error_message': 'Phase not synced - {{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_if_need_phase_fetch

        check_if_need_phase_fetch >> rail.Label(
            'Yes') >> fetch_phase_data >> prepare_phase_data >> check_if_phase_exists
        check_if_need_phase_fetch >> rail.Label(
            'No') >> prepare_phase_data >> check_if_phase_exists

        check_if_phase_exists >> rail.Label(
            'Yes') >> update_phase_to_procore >> check_if_has_categories
        check_if_phase_exists >> rail.Label(
            'No') >> create_phase_to_procore >> check_if_has_categories

        check_if_has_categories >> rail.Label(
            'Yes') >> trigger_categories_sync_child_dag >> wait_for_categories_completion >> catch_error
        check_if_has_categories >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
