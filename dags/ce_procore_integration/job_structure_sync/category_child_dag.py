from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.category_child_dag_id,
        description='Computerease to Procore Category sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.category_child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_if_category_exists',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_category_exists = rail.IfOperator(
            task_id='check_if_category_exists',
            test=lambda dag_run: dag_run.conf['category_data']['id'] is not None,
            yes_task='update_category_to_procore',
            no_task='create_category_to_procore'
        )

        def get_create_or_update_category_payload(dag_run):
            category_data = dag_run.conf['category_data']

            name = category_data.get('description', '').strip()
            if not name:
                name = category_data.get('code', '')
            if not name:
                raise ValueError(
                    "Category must have either description or code for sync")

            payload = {
                "name": name,
                "code": category_data.get('code', ''),
                "parent_id": dag_run.conf.get('phase_parent_id') if dag_run.conf.get('parent_type') == 'phase' else None,
            }

            return payload

        update_category_to_procore = rail.ProcoreApiOperator(
            task_id='update_category_to_procore',
            endpoint='/projects/{{ dag_run.conf.procore_project_id }}/work_breakdown_structure/segments/{{ dag_run.conf.cost_code_segment_id }}/segment_items/{{ dag_run.conf.category_data.id }}',
            method='PATCH',
            data=get_create_or_update_category_payload
        )

        create_category_to_procore = rail.ProcoreApiOperator(
            task_id='create_category_to_procore',
            endpoint='/projects/{{ dag_run.conf.procore_project_id }}/work_breakdown_structure/segments/{{ dag_run.conf.cost_code_segment_id }}/segment_items',
            method='POST',
            data=get_create_or_update_category_payload
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'CATEGORY',
                'entity_code': dag_run.conf.get('category_code', ''),
                'full_code': f"{dag_run.conf.get('job_code', '')}{('.' + dag_run.conf.get('phase_code', '')) if dag_run.conf.get('phase_code') else ''}.{dag_run.conf.get('category_code', '')}",
                'entity_name': dag_run.conf['category_data'].get('description', ''),
                'error_message': 'Category not synced - {{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_if_category_exists

        check_if_category_exists >> rail.Label(
            'Yes') >> update_category_to_procore >> catch_error
        check_if_category_exists >> rail.Label(
            'No') >> create_category_to_procore >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
