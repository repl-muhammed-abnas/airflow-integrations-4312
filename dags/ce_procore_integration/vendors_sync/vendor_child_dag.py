from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.vendor_child_dag_id,
        description='Computerease to Procore vendor sync Child DAG',
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
            start_task='prepare_payload',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def resolve_procore_vendor_id(vendor_data):
            """
            Returns (vendor_id, error_message). Error means this vendor must be skipped.
            """
            matching_vendors = vendor_data.get('matching_vendors')
            if not matching_vendors:
                return None, None
            if len(matching_vendors) == 1:
                return matching_vendors[0]['id'], None
            synced_vendors = [v for v in matching_vendors if v.get('origin_id')]
            if synced_vendors:
                return synced_vendors[0]['id'], None
            return None, (
                f"Multiple Procore vendors found with code {vendor_data.get('code', '')}"
            )

        def build_payload(dag_run):
            vendors = dag_run.conf.get('vendors', [])
            updates = []
            pre_sync_errors = []

            for vendor_data in vendors:
                code = vendor_data.get('code', '')
                name = vendor_data.get('name', '')

                if not name:
                    pre_sync_errors.append({
                        'entity_code': code,
                        'entity_name': name,
                        'error_message': 'Vendor not synced - Vendor Name is required'
                    })
                    continue

                vendor_id, lookup_error = resolve_procore_vendor_id(vendor_data)
                if lookup_error:
                    pre_sync_errors.append({
                        'entity_code': code,
                        'entity_name': name,
                        'error_message': f'Vendor not synced - {lookup_error}'
                    })
                    continue

                update = {
                    'origin_id': f"CE_{code}" if code else None,
                    'abbreviated_name': code,
                    'name': name,
                    'address': vendor_data.get('address', ''),
                    'city': vendor_data.get('city', ''),
                    'state_code': vendor_data.get('state', ''),
                    'zip': vendor_data.get('zip', ''),
                    'business_phone': vendor_data.get('phone', ''),
                    'fax_number': vendor_data.get('fax', ''),
                    'email_address': vendor_data.get('email', ''),
                    'website': vendor_data.get('website', ''),
                    'is_active': vendor_data.get('is_active', True),
                    'country_code': config.country_code
                }
                if vendor_id is not None:
                    update['id'] = vendor_id
                updates.append(update)

            return {
                'company_id': dag_run.conf['procore_company_id'],
                'updates': updates,
                'pre_sync_errors': pre_sync_errors
            }

        prepare_payload = rail.PythonOperator(
            task_id='prepare_payload',
            python_callable=build_payload
        )

        if_has_pre_sync_errors = rail.IfOperator(
            task_id='if_has_pre_sync_errors',
            test='{{ result("prepare_payload").pre_sync_errors | length > 0 }}',
            yes_task='log_pre_sync_errors',
            no_task='if_has_valid_updates'
        )

        log_pre_sync_errors = rail.WriteLogOperator(
            task_id='log_pre_sync_errors',
            message='na',
            severity='Error/Exception',
            items=lambda: rail.result('prepare_payload')['pre_sync_errors'],
            properties=lambda item: item
        )

        if_has_valid_updates = rail.IfOperator(
            task_id='if_has_valid_updates',
            test='{{ result("prepare_payload").updates | length > 0 }}',
            yes_task='sync_vendors_to_procore',
            no_task='catch_error'
        )

        sync_vendors_to_procore = rail.ProcoreApiOperator(
            task_id='sync_vendors_to_procore',
            endpoint='/vendors/sync',
            method='PATCH',
            data=lambda: {
                'company_id': rail.result('prepare_payload')['company_id'],
                'updates': rail.result('prepare_payload')['updates']
            },
            query_params={
                'run_configurable_validations': 'false'
            }
        )

        if_sync_has_errors = rail.IfOperator(
            task_id='if_sync_has_errors',
            test='{{ (result("sync_vendors_to_procore").errors or []) | length > 0 }}',
            yes_task='log_sync_failures',
            no_task='catch_error'
        )

        def get_error_message(error_object):
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
                    messages.append(
                        f"{key} {error_object.get(key,'')}: {msg_str}")
                return "Vendor not created/updated due to - " + "; ".join(messages)
            except Exception as e:  # pylint: disable=broad-except
                return f"Error parsing error message: {str(e)}"

        def build_sync_failure_log_items(dag_run):
            vendors = dag_run.conf.get('vendors', [])
            vendor_by_origin_id = {
                f"CE_{v.get('code', '')}": v for v in vendors if v.get('code')
            }
            errors = (rail.result('sync_vendors_to_procore') or {}).get('errors', [])
            items = []
            for err in errors:
                vendor = vendor_by_origin_id.get(err.get('origin_id'), {})
                items.append({
                    'entity_code': vendor.get('code', ''),
                    'entity_name': vendor.get('name', ''),
                    'error_message': get_error_message(err)
                })
            return items

        log_sync_failures = rail.WriteLogOperator(
            task_id='log_sync_failures',
            message='na',
            severity='Error/Exception',
            items=build_sync_failure_log_items,
            properties=lambda item: item
        )

        def build_catch_error_properties(dag_run):
            vendors = dag_run.conf.get('vendors', [])
            error_msg = rail.render_template('{{ get_error_message() }}')
            if len(vendors) == 1:
                vendor = vendors[0]
                return {
                    'entity_code': vendor.get('code', ''),
                    'entity_name': vendor.get('name', ''),
                    'error_message': f"Vendor not synced - {error_msg}"
                }
            return {
                'entity_code': '',
                'entity_name': '',
                'error_message': f"One or more vendors not synced - {error_msg}"
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=build_catch_error_properties
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> catch_error
        batch_task >> prepare_payload >> if_has_pre_sync_errors

        if_has_pre_sync_errors >> rail.Label(
            'Yes') >> log_pre_sync_errors >> if_has_valid_updates
        if_has_pre_sync_errors >> rail.Label('No') >> if_has_valid_updates

        if_has_valid_updates >> rail.Label(
            'Yes') >> sync_vendors_to_procore >> if_sync_has_errors
        if_has_valid_updates >> rail.Label('No') >> catch_error

        if_sync_has_errors >> rail.Label(
            'Yes') >> log_sync_failures >> catch_error
        if_sync_has_errors >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
