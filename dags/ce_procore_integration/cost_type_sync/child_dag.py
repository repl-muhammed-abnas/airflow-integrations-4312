from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Sync Cost Type to Procore',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_if_cost_type_exists',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_cost_type_exists = rail.IfOperator(
            task_id='check_if_cost_type_exists',
            test=lambda dag_run: bool(
                dag_run.conf['cost_type'].get('procore_id', False)),
            yes_task='update_procore_cost_type',
            no_task='create_procore_cost_type'
        )

        def get_create_or_update_cost_type_payload(dag_run):
            return {
                'name': dag_run.conf['cost_type']['name'],
                'code': dag_run.conf['cost_type']['code']
            }

        update_procore_cost_type = rail.ProcoreApiOperator(
            task_id='update_procore_cost_type',
            endpoint='/companies/{{ dag_run.conf.company_id }}/work_breakdown_structure/segments/{{ dag_run.conf.segment_id }}/segment_items/{{ dag_run.conf.cost_type.procore_id }}',
            method='PATCH',
            data=get_create_or_update_cost_type_payload
        )

        create_procore_cost_type = rail.ProcoreApiOperator(
            task_id='create_procore_cost_type',
            endpoint='/companies/{{ dag_run.conf.company_id }}/work_breakdown_structure/segments/{{ dag_run.conf.segment_id }}/segment_items',
            method='POST',
            data=get_create_or_update_cost_type_payload
        )

        def get_error_message():
            err = rail.render_template('{{ get_error_message() }}')
            if type(err) == str:
                status = 'Error'
                reason = err
            else:
                status = err['response']['status_code'] \
                    if err.get('response') else 'Error'
                reason = err['response']['json']['error']['reason'] \
                    if err.get('response') else err

            return {
                'code': rail.render_template('{{ dag_run.conf.cost_type.code }}'),
                'name': rail.render_template('{{ dag_run.conf.cost_type.name }}'),
                'status': status,
                'reason': reason
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_message
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_if_cost_type_exists

        check_if_cost_type_exists >> rail.Label(
            'Yes') >> update_procore_cost_type >> catch_error
        check_if_cost_type_exists >> rail.Label(
            'No') >> create_procore_cost_type >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
