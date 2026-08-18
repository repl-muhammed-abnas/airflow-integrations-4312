from datetime import timedelta
import rail
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.change_order_line_item_sync_dag_id,
        description='Subcontract Change Order Line Item Sync - Process Individual Change Order Line Item to Procore',
        max_active_runs=config.line_item_sync_dag_max_active_runs,
        integration_type='generic',
        company_key=config.instance,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_wbs_code',
            end_task='catch_unhandled_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_wbs_code(dag_run):
            line_item = dag_run.conf['change_order_line_item']
            cost_types = dag_run.conf['cost_types']
            phase = line_item['phase_code']
            category = line_item['category_code']
            costtype = line_item['costtype']
            cost_type = ''
            wbs_code = ''
            for entry in cost_types:
                if entry['code'] == costtype:
                    cost_type = entry['reference']
                    break

            if phase and category and cost_type:
                wbs_code = f'{phase}-{category}.{cost_type}'
            elif phase and cost_type:
                wbs_code = f'{phase}.{cost_type}'
            elif category and cost_type:
                wbs_code = f'{category}.{cost_type}'
            elif cost_type:
                wbs_code = cost_type
            
            return {
                        'phase_code': phase,
                        'category_code': category,
                        'cost_type': cost_type,
                        'flat_code': wbs_code
                    }

        fetch_wbs_code = rail.PythonOperator(
            task_id='fetch_wbs_code',
            python_callable=get_wbs_code
        )

        def get_wbs_code_id(dag_run):
            wbs_code_id = ''
            wbs_code = rail.result('fetch_wbs_code')['flat_code']
            project_wbs_codes = dag_run.conf['project_wbs_codes']
            for project_wbs_code in project_wbs_codes:
                if normalize_ce_identifier(project_wbs_code['flat_code']) == wbs_code:
                    wbs_code_id = project_wbs_code['id']
            return wbs_code_id
        
        fetch_wbs_code_id = rail.PythonOperator(
            task_id='fetch_wbs_code_id',
            python_callable=get_wbs_code_id
        )

        has_missing_wbs_code = rail.IfOperator(
            task_id='has_missing_wbs_code',
            test="{{ result('fetch_wbs_code_id') == '' }}",
            yes_task='trigger_wbs_creation',
            no_task='create_commitment_change_order_line_item'
        )

        trigger_wbs_creation = rail.TriggerDagRunOperator(
            task_id='trigger_wbs_creation',
            trigger_dag_id=config.wbs_code_creator_dag_id,
            conf=lambda dag_run: {
                'project_id': dag_run.conf['project_id'],
                'wbs_codes_to_create': [ rail.result('fetch_wbs_code') ]
            }
        )

        wait_for_wbs_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_wbs_creation',
            dag_runs='{{ result("trigger_wbs_creation") }}',
            execution_timeout=timedelta(minutes=30)
        )

        gather_created_wbs_codes = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_created_wbs_codes',
            dag_runs='{{ result("trigger_wbs_creation") }}',
            dagrun_task_id='compile_results'
        )
        
        def get_create_commitment_change_order_line_item_payload(dag_run):
            line_item = dag_run.conf['change_order_line_item']
            wbs_code_id = rail.result('fetch_wbs_code_id') or ''

            if wbs_code_id == '':
                wbs_result = rail.result('gather_created_wbs_codes', [])
                for result in wbs_result:
                    if result and isinstance(result, dict):
                        wbs_code_id = result.get(rail.result('fetch_wbs_code')['flat_code'], wbs_code_id)

            if line_item['units'] is not None and line_item['unit_price'] is not None:
                payload = {
                    "wbs_code_id": wbs_code_id,
                    "amount": line_item['amount'],
                    "description": line_item['description'],
                    "quantity": line_item['units'],
                    "unit_cost": line_item['unit_price']
                }

            else:
                payload = {
                    "wbs_code_id": wbs_code_id,
                    "amount": line_item['amount'],
                    "description": line_item['description']
                }

            dag_run.conf['CCO_line_item_Payload'] = payload

            return payload

        create_commitment_change_order_line_item = rail.ProcoreApiOperator(
            task_id='create_commitment_change_order_line_item',
            endpoint='/companies/{{dag_run.conf["company_id"]}}/projects/{{dag_run.conf["project_id"]}}/commitment_change_orders/{{dag_run.conf["commitment_change_order_id"]}}/line_items',
            method='POST',
            version='2.0',
            data=get_create_commitment_change_order_line_item_payload
        )

        def get_error_details(dag_run):
            reason = ''
            payload = {}
            try:
                payload = dag_run.conf.get('CCO_line_item_Payload', {})

                err = rail.render_template('{{ get_error_message() }}')
                if type(err) == str:
                    status = 'Error'
                    reason += err
                else:
                    status = err['response']['status_code'] \
                        if err.get('response') else 'Error'
                    reason += err['response']['json']['error']['reason'] \
                        if err.get('response') else err
            except Exception as e:
                status = "An exception occurred"
                reason = str(e)

            return {
                'rfc_code': dag_run.conf['rfc_code'],
                'project_id': dag_run.conf['project_id'],
                'commitment_contract_id': dag_run.conf['commitment_contract_id'],
                'CCO_payload': dag_run.conf['CCO_payload'],
                'line_item_payload': payload,
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
        batch_task >> fetch_wbs_code >> fetch_wbs_code_id >> has_missing_wbs_code
        has_missing_wbs_code >> rail.Label(
            'Yes') >> trigger_wbs_creation >> wait_for_wbs_creation >> gather_created_wbs_codes >> create_commitment_change_order_line_item
        has_missing_wbs_code >> rail.Label(
            'No') >> create_commitment_change_order_line_item
        create_commitment_change_order_line_item >> catch_unhandled_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
