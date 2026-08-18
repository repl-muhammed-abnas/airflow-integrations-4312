from datetime import timedelta, datetime
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.budget_line_item_dag_id,
        description='Procore Job Structure Webhook Events Processing - Budget Line Item DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'aws_conn_id': config.aws_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_budget_line_item',
            end_task='catch_unhandled_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_budget_line_item = rail.ProcoreApiOperator(
            task_id='get_budget_line_item',
            endpoint='/budget_line_items/{{ dag_run.conf.budget_line_item_id }}?project_id={{ dag_run.conf.project_id }}',
            method='GET',
            version='1.1'
        )

        def get_error_details(dag_run):
            try:
                reason = ''
                err = rail.render_template('{{ get_error_message() }}')
                if type(err) == str:
                    status = 'Error'
                    reason += err
                else:
                    status = err['response']['status_code'] \
                        if err.get('response') else 'Error'
                    reason += err['response']['json']['error']['reason'] \
                        if err.get('response') else err
            except:
                status = "An exception occurred"

            return {
                'entity_type': 'BUDGET',
                'procore_project_id': dag_run.conf['project_id'],
                'procore_project_name': dag_run.conf['project_name'],
                'budget_line_item_ids': dag_run.conf['budget_line_item_id'],
                'error_message': reason
            }

        catch_unhandled_error = rail.WriteLogOperator(
            task_id='catch_unhandled_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_unhandled_error >> log_to_sumo
        batch_task >> get_budget_line_item >> catch_unhandled_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
