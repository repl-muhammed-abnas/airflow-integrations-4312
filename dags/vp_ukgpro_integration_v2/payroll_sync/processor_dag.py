# pylint: disable=too-many-statements
import uuid
import logging
from datetime import datetime, timedelta
import rail  # pylint: disable=import-error
from vp_ukgpro_integration_v2.payroll_sync.utils.config_helper import (
    extract_dynamic_config_from_dag_run
)
from vp_ukgpro_integration_v2.payroll_sync.utils.error_handler import (
    capture_processor_error
)


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_payroll_sync_v2_processor_{config.instance}',
        description='Processes VantagePoint timesheet data and sends to UKG Pro',
        integration_type='generic',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,  # Triggered by webhook
        tags=['vantagepoint_ukgpro', 'payroll_sync', 'processor'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'retries': config.max_retries,
            'retry_delay': timedelta(minutes=config.retry_delay_minutes),
        }
    ) as dag:

        extract_dynamic_config = rail.PythonOperator(
            task_id='extract_dynamic_config',
            python_callable=lambda dag_run: extract_dynamic_config_from_dag_run(dag_run, config)
        )

        def extract_timesheet_data_from_webhook(dag_run):
            # Get the full dag_run.conf directly from the dag_run parameter
            dag_conf = dag_run.conf if dag_run else None

            # Extract webhook data - support both webhook-based and direct triggers
            webhook_data = None
            if dag_conf:
                if 'webhook' in dag_conf and 'data' in dag_conf['webhook']:
                    webhook_data = dag_conf['webhook']['data']
                elif 'original_webhook_data' in dag_conf:
                    webhook_data = dag_conf['original_webhook_data']
                elif 'data' in dag_conf:
                    webhook_data = dag_conf['data']
                else:
                    # Direct webhook data (array at root level)
                    webhook_data = dag_conf

            if not webhook_data:
                raise ValueError(
                    f"No webhook data received. dag_run.conf: {dag_conf}")

            logging.info(
                "Extracted %s entries from webhook data",
                len(webhook_data) if isinstance(webhook_data, list) else 1)

            timesheet_entries = []

            if isinstance(webhook_data, list):
                timesheet_entries = webhook_data
            elif isinstance(webhook_data, dict) and 'timesheets' in webhook_data:
                timesheet_entries = webhook_data['timesheets']
            elif isinstance(webhook_data, dict):
                timesheet_entries = [webhook_data]

            return {
                'entries': timesheet_entries,
                'count': len(timesheet_entries),
                'received_at': dag_conf.get('webhook', {}).get('received_at', '')
            }

        extract_timesheet_data = rail.PythonOperator(
            task_id='extract_timesheet_data',
            python_callable=extract_timesheet_data_from_webhook
        )

        if_data_present = rail.IfOperator(
            task_id='if_data_present',
            test=lambda: rail.result(
                'extract_timesheet_data').get('count', 0) > 0,
            yes_task='validate_and_transform'
        )

        def validate_and_transform_data():
            data = rail.result('extract_timesheet_data')
            entries = data['entries']

            validated_entries = []
            validation_errors = []

            for idx, entry in enumerate(entries):
                try:
                    # VantagePoint field names (no spaces)
                    company_code = entry.get('CompanyCode')
                    emp_no = entry.get('FileNumber')

                    # Determine entry type: Hours, Earnings, or Memo
                    hours_code = entry.get('Hours3Code')
                    hours_amt = entry.get('Hours3Amt', 0)
                    earnings_code = entry.get('Earnings3Code')
                    earnings_amt = entry.get('Earnings3Amt', 0)
                    memo_code = entry.get('MemoCode')
                    memo_amt = entry.get('MemoAmt', 0)

                    # Validate required fields
                    if not company_code:
                        raise ValueError("CompanyCode is required")
                    if not emp_no:
                        raise ValueError("FileNumber is required")

                    # Process based on entry type
                    # Use current date as trigger_date if not provided
                    trigger_date = entry.get(
                        'trigger_date') or datetime.now().isoformat()

                    if hours_code and float(hours_amt) > 0:
                        # Hours entry
                        validated_entries.append({
                            'company_code': str(company_code),
                            'emp_no': str(emp_no),
                            'hours_code': str(hours_code),
                            'hours_amt': float(hours_amt),
                            'entry_type': 'hours',
                            'trigger_date': trigger_date,
                            'original_entry': entry
                        })
                    elif earnings_code and float(earnings_amt) > 0:
                        # Earnings entry (treat as hours for now)
                        validated_entries.append({
                            'company_code': str(company_code),
                            'emp_no': str(emp_no),
                            'hours_code': str(earnings_code),
                            'hours_amt': float(earnings_amt),
                            'entry_type': 'earnings',
                            'trigger_date': trigger_date,
                            'original_entry': entry
                        })
                    elif memo_code and float(memo_amt) > 0:
                        # Memo entry (treat as hours for now)
                        validated_entries.append({
                            'company_code': str(company_code),
                            'emp_no': str(emp_no),
                            'hours_code': str(memo_code),
                            'hours_amt': float(memo_amt),
                            'entry_type': 'memo',
                            'trigger_date': trigger_date,
                            'original_entry': entry
                        })
                    else:
                        raise ValueError(
                            "Entry must have Hours3Code/Amt, Earnings3Code/Amt, or MemoCode/Amt with value > 0")

                except Exception as e:
                    validation_errors.append({
                        'index': idx,
                        'entry': entry,
                        'error': str(e)
                    })

            logging.info(
                "Validation complete: %s valid entries, "
                "%s errors out of %s total entries",
                len(validated_entries),
                len(validation_errors),
                len(entries)
            )

            if validation_errors:
                logging.warning("Validation errors: %s", validation_errors)

            # If no valid entries, raise an error instead of just returning empty
            if len(validated_entries) == 0:
                error_summary = "; ".join([f"Entry {e['index']}: {e['error']}" for e in validation_errors[:5]])
                raise ValueError(
                    f"No valid entries found. All {len(entries)} entry(ies) failed validation. "
                    f"Errors: {error_summary}"
                )

            return {
                'validated_entries': validated_entries,
                'validation_errors': validation_errors,
                'total_entries': len(entries),
                'valid_count': len(validated_entries),
                'error_count': len(validation_errors)
            }

        validate_and_transform = rail.PythonOperator(
            task_id='validate_and_transform',
            python_callable=validate_and_transform_data
        )

        if_validation_passed = rail.IfOperator(
            task_id='if_validation_passed',
            test=lambda: rail.result('validate_and_transform').get(
                'valid_count', 0) > 0,
            yes_task='create_employee_list'
        )

        def create_employee_list_for_lookup():
            data = rail.result('validate_and_transform')
            unique_employees = set()

            for entry in data['validated_entries']:
                unique_employees.add((entry['company_code'], entry['emp_no']))

            employee_list = [{'company_code': cc, 'emp_no': en}
                             for cc, en in unique_employees]

            return {
                'employees': employee_list,
                'count': len(employee_list)
            }

        create_employee_list = rail.PythonOperator(
            task_id='create_employee_list',
            python_callable=create_employee_list_for_lookup
        )

        def build_validation_items():
            """Return employees to validate, or [] if validation is disabled.

            Driving the foreach off this list lets the foreach short-circuit
            cleanly when validation is opted-out per-instance, instead of
            iterating and skipping inside each lookup task.
            """
            dynamic_config = rail.result('extract_dynamic_config')
            if not dynamic_config.get('validate_employees', False):
                return []
            return rail.result('create_employee_list').get('employees', [])

        build_validation_items_task = rail.PythonOperator(
            task_id='build_validation_items',
            python_callable=build_validation_items
        )

        clear_employee_validation_results = rail.SetVariableOperator(
            task_id='clear_employee_validation_results',
            append=False,
            name='employee_validation_results',
            value=lambda: []
        )

        lookup_employee = rail.UKGProGenericOperator(
            task_id='lookup_employee',
            ukgpro_conn_id=(
                "{{ result('extract_dynamic_config')['ukgpro_conn_id'] }}"
            ),
            endpoint=(
                "/personnel/v1/employee-changes/"
                "{{ result('validate_each_employee')['emp_no'] }}"
            ),
            method='GET',
            required_fields=['employeeNumber'],
            extract_from_array=False
        )

        def capture_validation_result_method():
            employee = rail.result('validate_each_employee')
            response = rail.result('lookup_employee')
            if response and response.get('employeeNumber'):
                return {'valid': True, 'employee': employee}
            return {
                'valid': False,
                'employee': employee,
                'reason': 'Employee not found in UKG Pro'
            }

        capture_validation_result = rail.PythonOperator(
            task_id='capture_validation_result',
            python_callable=capture_validation_result_method,
            trigger_rule='all_done'
        )

        store_validation_result = rail.SetVariableOperator(
            task_id='store_validation_result',
            append=True,
            name='employee_validation_results',
            value=lambda: rail.result('capture_validation_result')
        )

        validate_each_employee_end = rail.EmptyOperator(
            task_id='validate_each_employee_end'
        )

        validate_each_employee = rail.ForEachOperator(
            task_id='validate_each_employee',
            items="{{ result('build_validation_items') | to_json }}",
            start_task='lookup_employee',
            end_task='validate_each_employee_end'
        )

        def aggregate_validation_results():
            dynamic_config = rail.result('extract_dynamic_config')
            if not dynamic_config.get('validate_employees', False):
                return {
                    'all_valid': True,
                    'validated_employees': [],
                    'message': 'Employee validation disabled'
                }
            results = (
                rail.get_dag_run_var('employee_validation_results') or []
            )
            validated = [r['employee'] for r in results if r.get('valid')]
            invalid = [
                {
                    'employee': r['employee'],
                    'reason': r.get('reason', 'Unknown')
                }
                for r in results if not r.get('valid')
            ]
            return {
                'all_valid': len(invalid) == 0,
                'validated_employees': validated,
                'invalid_employees': invalid,
                'valid_count': len(validated),
                'invalid_count': len(invalid)
            }

        validate_employees = rail.PythonOperator(
            task_id='validate_employees',
            python_callable=aggregate_validation_results,
            trigger_rule='all_done'
        )

        if_employees_valid = rail.IfOperator(
            task_id='if_employees_valid',
            test=lambda: rail.result(
                'validate_employees').get('all_valid', False),
            yes_task='map_hours_codes'
        )

        def map_hours_codes_to_ukgpro():
            data = rail.result('validate_and_transform')
            entries = data['validated_entries']
            dynamic_config = rail.result('extract_dynamic_config')

            mapped_entries = []

            for idx, entry in enumerate(entries):
                hours_code = entry['hours_code']
                mapped_code = config.hours_code_mapping.get(
                    hours_code, hours_code)

                trigger_date = entry['trigger_date']
                try:
                    if isinstance(trigger_date, str):
                        date_obj = datetime.fromisoformat(
                            trigger_date.replace('Z', '+00:00'))
                    else:
                        date_obj = datetime.now()
                    charge_date_iso = date_obj.strftime(
                        '%Y-%m-%dT%H:%M:%S.000Z')
                except Exception:
                    charge_date_iso = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                mapped_entry = {
                    'refId': str(uuid.uuid4()),
                    'companyCode': entry['company_code'],
                    'empNo': entry['emp_no'],
                    'chargeDate': charge_date_iso,
                    'code': mapped_code,
                    'hours': entry['hours_amt'],
                    'source': dynamic_config.get('ukgpro_source', config.ukgpro_source)
                }

                logging.info(
                    "Mapped entry %s: Employee %s, "
                    "Code %s -> %s, "
                    "Hours: %s, Type: %s",
                    idx+1,
                    entry['emp_no'],
                    entry['hours_code'],
                    mapped_code,
                    entry['hours_amt'],
                    entry.get('entry_type', 'unknown')
                )

                mapped_entries.append(mapped_entry)

            logging.info("Mapped %s entries for UKG Pro", len(mapped_entries))

            return {
                'earnings': mapped_entries,
                'count': len(mapped_entries)
            }

        map_hours_codes = rail.PythonOperator(
            task_id='map_hours_codes',
            python_callable=map_hours_codes_to_ukgpro
        )

        def create_batches_of_earnings():
            data = rail.result('map_hours_codes')
            all_earnings = data['earnings']
            dynamic_config = rail.result('extract_dynamic_config')
            batch_size = dynamic_config.get('batch_size', config.batch_size)

            batches = []
            for i in range(0, len(all_earnings), batch_size):
                batch = all_earnings[i:i + batch_size]
                batches.append({
                    'batch_number': len(batches) + 1,
                    'earnings': batch,
                    'size': len(batch)
                })

            return {
                'batches': batches,
                'total_batches': len(batches),
                'total_earnings': len(all_earnings)
            }

        create_earnings_batches = rail.PythonOperator(
            task_id='create_earnings_batches',
            python_callable=create_batches_of_earnings
        )

        clear_batch_send_results = rail.SetVariableOperator(
            task_id='clear_batch_send_results',
            append=False,
            name='batch_send_results',
            value=lambda: []
        )

        def send_batch_method():
            """POST one batch of earnings to UKG Pro using UKGProHook directly.

            Bypasses UKGProTimePayrollImportOperator because its __init__
            requires a non-empty list literal and rejects callables/templates,
            so it can't be parametrized per-iteration inside a ForEach.
            Mirrors the operator's payload (refId per entry,
            x-correlation-id header, same endpoint) so request semantics
            are unchanged.
            """
            # pylint: disable=import-outside-toplevel
            from rail.hooks.ukgpro_hook import UKGProHook
            batch_data = rail.result('send_each_batch')
            dynamic_config = rail.result('extract_dynamic_config')
            correlation_id = str(uuid.uuid4())

            earnings_with_refs = [
                {**e, 'refId': e.get('refId') or str(uuid.uuid4())}
                for e in batch_data['earnings']
            ]

            client = UKGProHook(dynamic_config['ukgpro_conn_id'])
            try:
                client.make_request(
                    method='POST',
                    endpoint=(
                        '/services/payroll/v1/'
                        'import-pay-items/earnings'
                    ),
                    data={'earnings': earnings_with_refs},
                    additional_headers={
                        'x-correlation-id': correlation_id
                    }
                )
            except Exception as exc:
                raise Exception(  # pylint: disable=broad-exception-raised
                    f"Failed to send batch "
                    f"{batch_data['batch_number']} to UKG Pro. "
                    f"Correlation ID: {correlation_id}. Error: {exc}"
                ) from exc

            return {
                'batch_number': batch_data['batch_number'],
                'correlation_id': correlation_id,
                'size': batch_data['size'],
                'success': True,
                'total_earnings': len(earnings_with_refs)
            }

        send_batch = rail.PythonOperator(
            task_id='send_batch',
            python_callable=send_batch_method
        )

        store_batch_result = rail.SetVariableOperator(
            task_id='store_batch_result',
            append=True,
            name='batch_send_results',
            value=lambda: rail.result('send_batch')
        )

        send_each_batch_end = rail.EmptyOperator(
            task_id='send_each_batch_end'
        )

        send_each_batch = rail.ForEachOperator(
            task_id='send_each_batch',
            items=(
                "{{ result('create_earnings_batches')['batches']"
                " | to_json }}"
            ),
            start_task='send_batch',
            end_task='send_each_batch_end'
        )

        send_all_earnings_batches = rail.PythonOperator(
            task_id='send_all_earnings_batches',
            python_callable=lambda: (
                rail.get_dag_run_var('batch_send_results') or []
            )
        )

        def log_success_message():
            batch_results = rail.result('send_all_earnings_batches')

            total_sent = sum(r['size'] for r in batch_results)
            successful_batches = sum(
                1 for r in batch_results if r.get('success', False))
            failed_batches = len(batch_results) - successful_batches

            # All batches should be successful if we reach here (due to error handling above)
            if failed_batches > 0:
                raise Exception(
                    f"{failed_batches} batch(es) failed to send to UKG Pro. "
                    f"Check logs for correlation IDs: {[r['correlation_id'] for r in batch_results if not r.get('success')]}"
                )

            return {
                "status": "SUCCESS",
                "message": f"Successfully sent {total_sent} earnings to UKG Pro in {len(batch_results)} batch(es)",
                "batches_sent": len(batch_results),
                "batches_successful": successful_batches,
                "total_earnings": total_sent,
                "timestamp": datetime.now().isoformat(),
                "correlation_ids": [r['correlation_id'] for r in batch_results]
            }

        log_success = rail.PythonOperator(
            task_id='log_success',
            python_callable=log_success_message,
            trigger_rule='all_success'
        )

        log_failure = rail.WriteLogOperator(
            task_id='log_failure',
            message='VantagePoint to UKG Pro Payroll Sync Failed',
            severity='Error',
            trigger_rule='one_failed',
            properties=lambda: {
                'dag_id': rail.get_current_context()['dag'].dag_id,
                'dag_run_id': rail.get_current_context()['run_id'],
                'execution_date': rail.get_current_context()['execution_date'].isoformat(),
                'reason': 'DAG execution failed - check task logs for details'
            }
        )

        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='all_done',
            python_callable=capture_processor_error
        )

        # Task dependencies
        extract_dynamic_config >> extract_timesheet_data >> if_data_present

        if_data_present >> rail.Label(
            'Yes') >> validate_and_transform >> if_validation_passed

        if_validation_passed >> rail.Label(
            'Yes') >> create_employee_list >> build_validation_items_task >> clear_employee_validation_results >> validate_each_employee >> validate_employees >> if_employees_valid

        if_employees_valid >> rail.Label(
            'Yes') >> map_hours_codes >> create_earnings_batches >> clear_batch_send_results >> send_each_batch >> send_all_earnings_batches >> log_success

        [extract_dynamic_config, extract_timesheet_data, if_data_present, validate_and_transform, if_validation_passed,
         create_employee_list, build_validation_items_task, clear_employee_validation_results,
         validate_each_employee, validate_employees, if_employees_valid,
         map_hours_codes, create_earnings_batches, clear_batch_send_results,
         send_each_batch, send_all_earnings_batches] >> log_failure

        [log_success, log_failure] >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
