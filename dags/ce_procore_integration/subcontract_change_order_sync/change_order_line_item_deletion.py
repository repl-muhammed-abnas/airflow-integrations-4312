from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.change_order_line_item_deletion_dag_id,
        description='Subcontract Change Order Line Item Deletion - Process Individual Change Order Line Item Deletion In Procore',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.line_item_sync_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='delete_commitment_change_order_line_item',
            end_task='catch_unhandled_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        delete_commitment_change_order_line_item = rail.ProcoreApiOperator(
            task_id='delete_commitment_change_order_line_item',
            endpoint='companies/{{dag_run.conf["company_id"]}}/projects/{{dag_run.conf["project_id"]}}/commitment_change_orders/{{dag_run.conf["commitment_change_order_id"]}}/line_items/{{dag_run.conf["line_item_id"]}}',
            method='DELETE',
            version='2.0'
        )

        def get_error_details(dag_run):
            try:
                reason = f'Deletion of commitment contract line item id {dag_run.conf["line_item_id"]} for commitment contract id {dag_run.conf["commitment_change_order_id"]} failed'

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
                'rfc_code': dag_run.conf['rfc_code'],
                'project_id': dag_run.conf['project_id'],
                'commitment_contract_id': dag_run.conf['commitment_contract_id'],
                'CCO_payload': '',
                'line_item_payload': 'delete',
                'reason': reason
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
        batch_task >> delete_commitment_change_order_line_item >> catch_unhandled_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
