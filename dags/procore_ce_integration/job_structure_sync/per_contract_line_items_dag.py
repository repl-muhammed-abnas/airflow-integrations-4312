from datetime import timedelta
import rail


def create_dag_instance(config):
    per_contract_dag_id = config.prime_contract_line_items_dag_id.replace(
        'prime_contract_line_items', 'per_contract_line_items'
    )

    with rail.create_airflow_dag(
        dag_id=per_contract_dag_id,
        description='Fetch line items for a single prime contract',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=5,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_line_items',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_line_items = rail.ProcoreApiOperator(
            task_id='get_line_items',
            endpoint=lambda dag_run: f'/prime_contracts/{dag_run.conf["contract_id"]}/line_items?project_id={dag_run.conf["project_id"]}',
            method='GET'
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PRIME_CONTRACT',
                'procore_project_id': dag_run.conf['project_id'],
                'contract_id': dag_run.conf['contract_id'],
                'error_message': 'Failed to fetch prime contract line items: ' + rail.render_template('{{ get_error_message() }}')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_line_items >> catch_error
        batch_task >> catch_error
        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
