from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Subcontract Change Order Sync Child - Process Individual Change Order to Procore',
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
            start_task='has_project_id',
            end_task='catch_unhandled_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        has_project_id = rail.IfOperator(
            task_id='has_project_id',
            test=lambda dag_run:  dag_run.conf['project_id'] and dag_run.conf['project_id'] != None,
            yes_task='fetch_work_order_contract_id',
            no_task='has_sync_failed'
        )

        fetch_work_order_contract_id = rail.ProcoreApiOperator(
            task_id='fetch_work_order_contract_id',
            endpoint='/work_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data_handler=lambda resp, dag_run: [{
                'work_order_contract_id': work_order_contract['id']
            } for work_order_contract in resp
                if work_order_contract['origin_id'] == f"CE_{dag_run.conf['change_order']['code']}" and work_order_contract['status'].lower() == 'approved']
        )

        has_work_order_contract_id = rail.IfOperator(
            task_id='has_work_order_contract_id',
            test='{{ result("fetch_work_order_contract_id") | length > 0 }}',
            yes_task='fetch_project_wbs_codes',
            no_task='has_sync_failed'
        )

        fetch_project_wbs_codes = rail.ProcoreApiOperator(
            task_id='fetch_project_wbs_codes',
            endpoint='/projects/{{dag_run.conf["project_id"]}}/work_breakdown_structure/wbs_codes',
            method='GET'
        )

        fetch_all_commitment_change_orders = rail.ProcoreApiOperator(
            task_id='fetch_all_commitment_change_orders',
            endpoint='/projects/{{dag_run.conf["project_id"]}}/commitment_change_orders',
            method='GET'
        )

        def check_if_existing_change_order(dag_run):
            all_commitment_change_orders = rail.result(
                'fetch_all_commitment_change_orders')
            rfc_code = dag_run.conf['change_order']['rfc_code']
            work_order_contract_id = rail.result('fetch_work_order_contract_id')[
                0]['work_order_contract_id']
            for co in all_commitment_change_orders:
                if co['contract_id'] == work_order_contract_id and co['number'] == rfc_code:
                    dag_run.conf['commitment_change_order_id'] = co['id']
                    dag_run.conf['commitment_change_order_status'] = co['status']
                    dag_run.conf['is_existing_change_order'] = True
                    return True
            return False

        is_existing_change_order = rail.IfOperator(
            task_id='is_existing_change_order',
            test=lambda dag_run: check_if_existing_change_order(dag_run),
            yes_task='is_existing_change_order_approved',
            no_task='create_commitment_change_order'
        )

        is_existing_change_order_approved = rail.IfOperator(
            task_id='is_existing_change_order_approved',
            test=lambda dag_run: dag_run.conf['commitment_change_order_status'] == 'approved',
            yes_task='has_sync_failed',
            no_task='update_commitment_change_order'
        )

        update_commitment_change_order = rail.ProcoreApiOperator(
            task_id='update_commitment_change_order',
            endpoint='/projects/{{dag_run.conf["project_id"]}}/commitment_change_orders/{{dag_run.conf["commitment_change_order_id"]}}',
            method='PATCH',
            data=lambda dag_run: get_create_commitment_change_order_payload(
                dag_run)
        )

        fetch_all_line_items_for_change_order = rail.ProcoreApiOperator(
            task_id='fetch_all_line_items_for_change_order',
            endpoint='/companies/{{dag_run.conf["company_id"]}}/projects/{{dag_run.conf["project_id"]}}/commitment_change_orders/{{dag_run.conf["commitment_change_order_id"]}}/line_items',
            method='GET',
            version='2.0'
        )

        trigger_commitment_change_order_line_item_deletion = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_commitment_change_order_line_item_deletion',
            items=lambda: rail.result('fetch_all_line_items_for_change_order'),
            trigger_dag_id=config.change_order_line_item_deletion_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'company_id': dag_run.conf['company_id'],
                'project_id': dag_run.conf['project_id'],
                'commitment_change_order_id': dag_run.conf['commitment_change_order_id'],
                'line_item_id': item.get('id', ''),
                'rfc_code': dag_run.conf['change_order']['rfc_code'],
                'commitment_contract_id': rail.result('fetch_work_order_contract_id')[0]['work_order_contract_id']
            }
        )

        wait_for_commitment_change_order_line_items_deletion = rail.WaitForDagRunsSensor(
            task_id='wait_for_commitment_change_order_line_items_deletion',
            dag_runs='{{ result("trigger_commitment_change_order_line_item_deletion") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_create_commitment_change_order_payload(dag_run):
            change_order_change_reasons = dag_run.conf['change_order_change_reasons']
            change_order_change_reason_id = [item['id'] for item in change_order_change_reasons if item['change_reason'].lower(
            ) == config.change_order_change_reason.lower()]
            payload = {
                "change_order": {
                    "contract_id": rail.result('fetch_work_order_contract_id')[0]['work_order_contract_id'],
                    "change_order_change_reason_id": change_order_change_reason_id[0],
                    "description": dag_run.conf['change_order'].get('description', ''),
                    "invoiced_date": dag_run.conf['change_order'].get('entered_date', ''),
                    "title": dag_run.conf['change_order'].get('description', ''),
                    "status": "draft",
                    "number": dag_run.conf['change_order']['rfc_code'],
                    "signed_change_order_received_date": dag_run.conf['change_order'].get('approved_date', '')
                }
            }
            dag_run.conf['CCO_payload'] = payload
            status = getCCOStatus(dag_run)
            dag_run.conf['status'] = status
            return payload

        create_commitment_change_order = rail.ProcoreApiOperator(
            task_id='create_commitment_change_order',
            endpoint='/projects/{{dag_run.conf["project_id"]}}/commitment_change_orders',
            method='POST',
            data=get_create_commitment_change_order_payload
        )

        def get_commitment_change_order_id(dag_run):
            if 'commitment_change_order_id' in dag_run.conf:
                return dag_run.conf['commitment_change_order_id']
            else:
                dag_run.conf['commitment_change_order_id'] = rail.result(
                    'create_commitment_change_order')['id']
                return dag_run.conf['commitment_change_order_id']

        trigger_commitment_change_order_line_items_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_commitment_change_order_line_items_sync',
            items=lambda dag_run: dag_run.conf['change_order']['subcontract_item'],
            trigger_dag_id=config.change_order_line_item_sync_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'change_order_line_item': item,
                'company_id': dag_run.conf['company_id'],
                'commitment_change_order_id': get_commitment_change_order_id(dag_run),
                'project_id': dag_run.conf['project_id'],
                'cost_types': dag_run.conf['cost_types'],
                'project_wbs_codes': rail.result('fetch_project_wbs_codes'),
                'rfc_code': dag_run.conf['change_order']['rfc_code'],
                'commitment_contract_id': rail.result('fetch_work_order_contract_id')[0]['work_order_contract_id']
            }
        )

        wait_for_commitment_change_order_line_items_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_commitment_change_order_line_items_sync',
            dag_runs='{{ result("trigger_commitment_change_order_line_items_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def getCCOStatus(dag_run):
            status = dag_run.conf['change_order']['approval_status']
            status_to_return = 'draft'
            if status.lower() == 'approved':
                status_to_return = "approved"
            elif status.lower() == 'denied':
                status_to_return = "rejected"
            return status_to_return

        update_commitment_change_order_status = rail.ProcoreApiOperator(
            task_id='update_commitment_change_order_status',
            endpoint='/projects/{{dag_run.conf["project_id"]}}/commitment_change_orders/{{dag_run.conf["commitment_change_order_id"]}}',
            method='PATCH',
            data=lambda dag_run: {
                "change_order": {
                    "status": dag_run.conf['status']
                }
            }
        )

        def get_error_details(dag_run):
            try:
                project_id = dag_run.conf['project_id']
                CCO_payload = ''
                commitment_contract_id = 0
                reason = ''
                CCO_line_item_Payload = ''

                if not project_id:
                    reason = 'Project does not exist'
                elif len(rail.result('fetch_work_order_contract_id')) == 0:
                    reason = 'Contract details not found or contract is not approved'
                else:
                    if len(rail.result('fetch_work_order_contract_id')) > 0:
                        commitment_contract_id = rail.result('fetch_work_order_contract_id')[
                            0]['work_order_contract_id']

                    if 'CCO_payload' in dag_run.conf:
                        CCO_payload = dag_run.conf['CCO_payload']

                    if 'CCO_line_item_Payload' in dag_run.conf:
                        CCO_line_item_Payload = dag_run.conf['CCO_line_item_Payload']

                    if 'commitment_change_order_id' in dag_run.conf and 'commitment_change_order_status' in dag_run.conf and dag_run.conf['commitment_change_order_status'] == 'approved':
                        reason = 'Existing commitment change order in approved status cannot be modified'

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
                'rfc_code': dag_run.conf['change_order']['rfc_code'],
                'project_id': project_id,
                'commitment_contract_id': commitment_contract_id,
                'CCO_payload': CCO_payload,
                'line_item_payload': CCO_line_item_Payload,
                'reason': reason
            }

        def check_for_failure_and_reason(dag_run):
            project_id = dag_run.conf['project_id']
            reason = ''

            if not project_id:
                reason = 'Project does not exist'
            elif len(rail.result('fetch_work_order_contract_id')) == 0:
                reason = 'Contract details not found or contract is not approved'
            elif 'commitment_change_order_id' in dag_run.conf and 'commitment_change_order_status' in dag_run.conf and dag_run.conf['commitment_change_order_status'] == 'approved':
                reason = 'Existing commitment change order in approved status cannot be modified'
            return True if reason != '' else False

        has_sync_failed = rail.IfOperator(
            task_id='has_sync_failed',
            test=lambda dag_run: check_for_failure_and_reason(dag_run),
            yes_task='catch_error',
            no_task='catch_unhandled_error'
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

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
        batch_task >> has_project_id

        has_project_id >> rail.Label(
            'Yes') >> fetch_work_order_contract_id >> has_work_order_contract_id
        has_project_id >> rail.Label('No') >> has_sync_failed

        has_work_order_contract_id >> rail.Label(
            'Yes') >> fetch_project_wbs_codes >> fetch_all_commitment_change_orders >> is_existing_change_order
        has_work_order_contract_id >> rail.Label('No') >> has_sync_failed

        is_existing_change_order >> rail.Label(
            'Yes') >> is_existing_change_order_approved
        is_existing_change_order >> rail.Label('No') >> create_commitment_change_order >> trigger_commitment_change_order_line_items_sync \
            >> wait_for_commitment_change_order_line_items_sync >> update_commitment_change_order_status >> has_sync_failed

        is_existing_change_order_approved >> rail.Label(
            'Yes') >> has_sync_failed
        is_existing_change_order_approved >> rail.Label('No') >> update_commitment_change_order >> fetch_all_line_items_for_change_order >> trigger_commitment_change_order_line_item_deletion \
            >> wait_for_commitment_change_order_line_items_deletion >> trigger_commitment_change_order_line_items_sync \
            >> wait_for_commitment_change_order_line_items_sync >> update_commitment_change_order_status >> has_sync_failed

        has_sync_failed >> rail.Label(
            'Yes') >> catch_error >> catch_unhandled_error >> log_to_sumo
        has_sync_failed >> rail.Label(
            'No') >> catch_unhandled_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
