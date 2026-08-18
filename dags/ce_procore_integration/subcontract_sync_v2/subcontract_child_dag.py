from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.subcontract_child_dag_id,
        description='Computerease to Procore Subcontract Sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_vendor_exists',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )


        check_vendor_exists = rail.IfOperator(
            task_id='check_vendor_exists',
            test=lambda dag_run: dag_run.conf.get('procore_vendor') is not None,
            yes_task='sync_subcontract_to_procore',
            no_task='log_vendor_not_found'
        )

        log_vendor_not_found = rail.WriteLogOperator(
            task_id='log_vendor_not_found',
            message='Vendor not found in Procore',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'subcontract_code': dag_run.conf['subcontract_data'].get('code', ''),
                'vendor_code': dag_run.conf['subcontract_data'].get('vendor_code', ''),
                'job_code': dag_run.conf['subcontract_data'].get('job_code', ''),
                'error_message': f"Subcontract not synced because Vendor {dag_run.conf['subcontract_data'].get('vendor_code', '')} not found in Procore"
            }
        )

        def build_sync_subcontract_payload(dag_run):
            subcontract_data = dag_run.conf['subcontract_data']
            project_id = dag_run.conf['procore_project_id']
            vendor_id = dag_run.conf['procore_vendor']['id']

            procore_status = config.APPROVAL_STATUS_MAPPER.get(
                subcontract_data.get('approval_status', '').lower(), 'Draft')
            return {
                "project_id": project_id,
                "updates": [
                    {
                        "origin_id": f"CE_{subcontract_data.get('code')}",
                        "number": subcontract_data.get('code'),
                        "title": subcontract_data.get('description'),
                        "vendor_id": vendor_id,
                        "status": procore_status,
                        "contract_date": subcontract_data.get('contract_date'),
                        "contract_start_date": subcontract_data.get('actual_start_date') or subcontract_data.get('orig_start_date'),
                        "contract_estimated_completion_date": subcontract_data.get('orig_finish_date'),
                        "actual_completion_date": subcontract_data.get('actual_finish_date'),
                        "executed": procore_status == 'Approved',
                        "signed_contract_received_date": subcontract_data.get('approved_date'),
                        "issued_on_date": subcontract_data.get('entered_date'),
                        "accounting_method": config.subcontract_accounting_method,
                        "retainage_percent": subcontract_data.get('retention_percent')
                    }
                ]
            }

        sync_subcontract_to_procore = rail.ProcoreApiOperator(
            task_id='sync_subcontract_to_procore',
            endpoint='/work_order_contracts/sync',
            method='PATCH',
            data=build_sync_subcontract_payload,
            query_params={
                'run_configurable_validations': 'false'
            }
        )

        check_if_successful_sync = rail.IfOperator(
            task_id='check_if_successful_sync',
            test='{{ result("sync_subcontract_to_procore").entities | length > 0 }}',
            yes_task='check_has_line_items',
            no_task='log_sync_failure'
        )

        # Check if subcontract has line items to sync
        check_has_line_items = rail.IfOperator(
            task_id='check_has_line_items',
            test=lambda dag_run: len(
                dag_run.conf['subcontract_data'].get('subcontract_item', [])) > 0,
            yes_task='trigger_line_items_sync',
            no_task='catch_error'
        )

        # Trigger line items sync child DAG
        trigger_line_items_sync = rail.TriggerDagRunOperator(
            task_id='trigger_line_items_sync',
            trigger_dag_id=config.subcontract_line_items_child_dag_id,
            conf=lambda dag_run: {
                'subcontract_data': dag_run.conf['subcontract_data'],
                'subcontract_id': rail.result('sync_subcontract_to_procore')['entities'][0]['id'],
                'project_id': dag_run.conf['procore_project_id'],
                'procore_company_id': dag_run.conf['procore_company_id'],
                'ce_cost_type_map': dag_run.conf['ce_cost_type_map'],
                'wbs_codes_lookup': dag_run.conf.get('wbs_codes_lookup') or {}
            }
        )

        wait_for_line_items_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_line_items_sync',
            dag_runs='{{ result("trigger_line_items_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_error_message(error_object):
            """
            Extract error message from Procore API response
            """
            try:
                errors = error_object.get('errors', {})
                if not errors or not isinstance(errors, dict):
                    return ""
                messages = []
                for key, msgs in errors.items():
                    if isinstance(msgs, list):
                        msg_str = ", ".join(str(m) for m in msgs)
                    else:
                        msg_str = str(msgs)
                    messages.append(f"{key}: {msg_str}")
                return "Subcontract not synced due to - " + "; ".join(messages)
            except Exception as e:
                return f"Error parsing error message: {str(e)}"

        log_sync_failure = rail.WriteLogOperator(
            task_id='log_sync_failure',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'subcontract_code': dag_run.conf['subcontract_data'].get('code', ''),
                    'vendor_code': dag_run.conf['subcontract_data'].get('vendor_code', ''),
                    'job_code': dag_run.conf['subcontract_data'].get('job_code', ''),
                    'error_message': get_error_message(err)
                }
                for err in rail.result('sync_subcontract_to_procore').get('errors', [])
            ],
            properties=lambda item: item
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'subcontract_code': dag_run.conf['subcontract_data'].get('code', ''),
                'vendor_code': dag_run.conf['subcontract_data'].get('vendor_code', ''),
                'job_code': dag_run.conf['subcontract_data'].get('job_code', ''),
                'error_message': "Subcontract not synced - {{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> catch_error
        batch_task >> check_vendor_exists

        check_vendor_exists >> rail.Label(
            'Yes') >> sync_subcontract_to_procore >> check_if_successful_sync
        check_vendor_exists >> rail.Label(
            'No') >> log_vendor_not_found >> catch_error

        check_if_successful_sync >> rail.Label('Yes') >> check_has_line_items
        check_if_successful_sync >> rail.Label(
            'No') >> log_sync_failure >> catch_error

        check_has_line_items >> rail.Label(
            'Yes') >> trigger_line_items_sync >> wait_for_line_items_sync >> catch_error
        check_has_line_items >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
