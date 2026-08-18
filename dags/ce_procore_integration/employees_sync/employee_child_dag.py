from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.employee_child_dag_id,
        description='Computerease to Procore employee sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='prepare_payload',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def resolve_procore_user_id(employee_data):
            """
            Returns (user_id, error_message). Error means this employee must be skipped.
            """
            matching = employee_data.get('matching_employees')
            if not matching:
                return None, None
            if len(matching) == 1:
                return matching[0]['id'], None
            code = employee_data.get('code', '')
            expected_origin_id = f"CE_{code}" if code else None
            synced = [m for m in matching if m.get('origin_id') == expected_origin_id]
            if synced:
                return synced[0]['id'], None
            return None, (
                f"Multiple Procore users found with employee_id {code}.."
            )

        def build_payload(dag_run):
            employees = dag_run.conf.get('employees', [])
            updates = []
            pre_sync_errors = []

            for employee_data in employees:
                code = employee_data.get('code', '')
                full_name = employee_data.get('full_name', '')
                last_name = employee_data.get('last_name', '')
                email_address = employee_data.get('email_address', '')

                if not last_name:
                    pre_sync_errors.append({
                        'entity_code': code,
                        'entity_name': full_name,
                        'error_message': 'Employee not synced - Employee last name is required'
                    })
                    continue

                if not email_address:
                    pre_sync_errors.append({
                        'entity_code': code,
                        'entity_name': full_name,
                        'error_message': 'Employee not synced - Employee email is required'
                    })
                    continue

                user_id, lookup_error = resolve_procore_user_id(employee_data)
                if lookup_error:
                    pre_sync_errors.append({
                        'entity_code': code,
                        'entity_name': full_name,
                        'error_message': f'Employee not synced - {lookup_error}'
                    })
                    continue

                update = {
                    'origin_id': f"CE_{code}" if code else None,
                    'employee_id': code,
                    'name': full_name,
                    'first_name': employee_data.get('first_name', ''),
                    'last_name': last_name,
                    'email_address': email_address,
                    'is_active': employee_data.get('is_active', True),
                    'is_employee': True,
                    'country_code': config.country_code
                }
                if user_id:
                    update['id'] = user_id
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
            items=lambda dag_run: rail.result('prepare_payload')['pre_sync_errors'],
            properties=lambda item: item
        )

        if_has_valid_updates = rail.IfOperator(
            task_id='if_has_valid_updates',
            test='{{ result("prepare_payload").updates | length > 0 }}',
            yes_task='sync_employees_to_procore',
            no_task='catch_error'
        )

        sync_employees_to_procore = rail.ProcoreApiOperator(
            task_id='sync_employees_to_procore',
            endpoint="companies/{{dag_run.conf.procore_company_id}}/users/sync",
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
            test='{{ result("sync_employees_to_procore").errors | length > 0 }}',
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
                return "Employee not created/updated due to - " + "; ".join(messages)
            except Exception as e:  # pylint: disable=broad-except
                return f"Error parsing error message: {str(e)}"

        def build_sync_failure_log_items(dag_run):
            employees = dag_run.conf.get('employees', [])
            employee_by_origin_id = {
                f"CE_{e.get('code', '')}": e for e in employees if e.get('code')
            }
            errors = (rail.result('sync_employees_to_procore') or {}).get('errors', [])
            items = []
            for err in errors:
                employee = employee_by_origin_id.get(err.get('origin_id'), {})
                items.append({
                    'entity_code': employee.get('code', ''),
                    'entity_name': employee.get('full_name', ''),
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
            employees = dag_run.conf.get('employees', [])
            error_msg = rail.render_template('{{ get_error_message() }}')
            if len(employees) == 1:
                employee = employees[0]
                return {
                    'entity_code': employee.get('code', ''),
                    'entity_name': employee.get('full_name', ''),
                    'error_message': f"Employee not synced - {error_msg}"
                }
            return {
                'entity_code': '',
                'entity_name': '',
                'error_message': f"One or more employees not synced - {error_msg}"
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
            'Yes') >> sync_employees_to_procore >> if_sync_has_errors
        if_has_valid_updates >> rail.Label('No') >> catch_error

        if_sync_has_errors >> rail.Label(
            'Yes') >> log_sync_failures >> catch_error
        if_sync_has_errors >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
