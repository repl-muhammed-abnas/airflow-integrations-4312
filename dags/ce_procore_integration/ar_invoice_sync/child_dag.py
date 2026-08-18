import rail
import calendar
from datetime import timedelta
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='AR Invoice Sync Child - Process AR Invoices to Procore',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_prime_contract',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        fetch_prime_contract = rail.ProcoreApiOperator(
            task_id='fetch_prime_contract',
            endpoint='/prime_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['batch']['project_id'],
                'filters[origin_id]': 'CE_{{ dag_run.conf.batch.job_code }}'
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'origin_id', f'CE_{dag_run.conf["batch"]["job_code"]}', 'id', None)
        )

        is_prime_contract_exist = rail.IfOperator(
            task_id='is_prime_contract_exist',
            test='{{ result("fetch_prime_contract") | is_truthy }}',
            yes_task='fetch_prime_contract_sov',
            no_task='write_log_prime_contract_not_found'
        )

        fetch_prime_contract_sov = rail.ProcoreApiOperator(
            task_id='fetch_prime_contract_sov',
            endpoint='/prime_contracts/{{ result("fetch_prime_contract") }}/line_items',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['batch']['project_id']
            }
        )

        def validate_invoices_against_sov(dag_run):
            invoices_to_sync = []
            for invoice in dag_run.conf['batch']['ar_invoices']:
                period = invoice.get('period', '')
                period_start = None
                period_end = None

                if period:
                    try:
                        year, month = period.split('/')
                        year = int(year)
                        month = int(month)
                        period_start = f"{year:04d}-{month:02d}-01"
                        last_day = calendar.monthrange(year, month)[1]
                        period_end = f"{year:04d}-{month:02d}-{last_day:02d}"
                    except (ValueError, IndexError) as e:
                        print(f"Error parsing period '{period}': {e}")

                invoice_data = {
                    'invoice': invoice,
                    'period_start': period_start,
                    'period_end': period_end
                }
                invoices_to_sync.append(invoice_data)

            sov_line_items = rail.result('fetch_prime_contract_sov')

            sov_flat_codes = set()
            for sov_item in sov_line_items:
                flat_code = sov_item.get('wbs_code', {}).get('flat_code', '')
                sov_flat_codes.add(normalize_ce_identifier(flat_code))

            valid_invoices = []
            invalid_invoices = []

            for invoice_data in invoices_to_sync:
                invoice = invoice_data['invoice']
                invoice_number = invoice.get('invoice_number', '')
                budget_codes = invoice.get('budget_codes', [])

                all_matched = True
                missing_codes = []

                for bc in budget_codes:
                    phase = bc.get('phase', '')
                    category = bc.get('category', '')
                    cost_type = bc.get('cost_type', config.default_cost_type)

                    if phase and category:
                        flat_code = f"{phase}-{category}.{cost_type}"
                    elif phase or category:
                        flat_code = f"{phase or category}.{cost_type}"
                    else:
                        flat_code = ''

                    # Empty flat_code (no phase/category) is always valid
                    # Only validate non-empty flat_codes against SOV
                    if flat_code and flat_code not in sov_flat_codes:
                        all_matched = False
                        missing_codes.append({
                            'flat_code': flat_code,
                            'phase': phase,
                            'category': category,
                            'net_amount': bc.get('net_amount', 0)
                        })

                if all_matched:
                    valid_invoices.append(invoice_data)
                else:
                    invalid_invoices.append({
                        'invoice_number': invoice_number,
                        'job_code': invoice.get('job_code', ''),
                        'customer_code': invoice.get('customer_code', ''),
                        'missing_codes': missing_codes,
                        'reason': f"Missing SOV line items for {len(missing_codes)} budget code(s)"
                    })

            return {
                'valid_invoices': valid_invoices,
                'invalid_invoices': invalid_invoices
            }

        validate_invoices = rail.PythonOperator(
            task_id='validate_invoices',
            python_callable=validate_invoices_against_sov
        )

        has_valid_invoices = rail.IfOperator(
            task_id='has_valid_invoices',
            test='{{ result("validate_invoices").valid_invoices | length > 0 }}',
            yes_task='trigger_owner_invoice_sync',
            no_task='has_invalid_invoices'
        )

        trigger_owner_invoice_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_owner_invoice_sync',
            items=lambda: rail.result(
                'validate_invoices').get('valid_invoices', []),
            trigger_dag_id=config.invoice_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                'invoice_data': item,
                'prime_contract_id': rail.result('fetch_prime_contract'),
                'project_id': dag_run.conf['batch']['project_id'],
                'company_id': dag_run.conf['company_id']
            }
        )

        wait_for_invoice_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_invoice_dags',
            dag_runs='{{ result("trigger_owner_invoice_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_prime_contract_not_found_properties(dag_run):
            batch = dag_run.conf.get('batch', {})
            invoice_numbers = ','.join(
                [x.get('invoice_number', '') for x in batch.get('ar_invoices', [])])
            return {
                'code': invoice_numbers,
                'job_code': batch.get('job_code', 'unknown'),
                'customer_code': batch.get('customer_code', 'unknown'),
                'company_id': dag_run.conf.get('company_id', 'unknown'),
                'status': 'Exception',
                'reason': f"Prime Contract not found for customer_code '{batch.get('customer_code', 'unknown')}' in project_id '{batch.get('project_id', 'unknown')}'"
            }

        write_log_prime_contract_not_found = rail.WriteLogOperator(
            task_id='write_log_prime_contract_not_found',
            message='Prime Contract Not Found',
            severity='Error/Exception',
            properties=get_prime_contract_not_found_properties
        )

        has_invalid_invoices = rail.IfOperator(
            task_id='has_invalid_invoices',
            test='{{ result("validate_invoices").invalid_invoices | length > 0 }}',
            yes_task='write_log_invoice_items_not_found',
            no_task='catch_error'
        )

        def get_invalid_invoice_properties(dag_run, item):
            return {
                'code': item.get('invoice_number', 'unknown'),
                'job_code': item.get('job_code', 'unknown'),
                'customer_code': item.get('customer_code', 'unknown'),
                'company_id': dag_run.conf.get('company_id', 'unknown'),
                'status': 'Exception',
                'reason': item.get('reason', ''),
                'missing_flat_codes': ', '.join([mc['flat_code'] for mc in item.get('missing_codes', [])])
            }

        write_log_invoice_items_not_found = rail.WriteLogOperator(
            task_id='write_log_invoice_items_not_found',
            message='Prime Contract SOV not found',
            severity='Error/Exception',
            properties=get_invalid_invoice_properties,
            items=lambda: rail.result('validate_invoices').get(
                'invalid_invoices', [])
        )

        def get_error_details(dag_run):
            batch = dag_run.conf.get('batch', {})
            invoice_numbers = ','.join(
                [x.get('invoice_number', '') for x in batch.get('ar_invoices', [])])

            err = rail.render_template('{{ get_error_message() }}')
            if isinstance(err, str):
                status = 'Error'
                reason = err
            else:
                status = err.get('response', {}).get('status_code', 'Error')
                reason = err.get('response', {}).get('json', {}).get(
                    'error', {}).get('reason', str(err))

            return {
                'code': invoice_numbers,
                'job_code': batch.get('job_code', 'unknown'),
                'customer_code': batch.get('customer_code', 'unknown'),
                'company_id': dag_run.conf.get('company_id', 'unknown'),
                'status': status,
                'reason': reason
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
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

        batch_task >> catch_error >> log_to_sumo
        batch_task >> fetch_prime_contract >> is_prime_contract_exist

        is_prime_contract_exist >> rail.Label(
            'No') >> write_log_prime_contract_not_found >> catch_error
        is_prime_contract_exist >> rail.Label(
            'Yes') >> fetch_prime_contract_sov >> validate_invoices >> has_valid_invoices

        has_valid_invoices >> rail.Label(
            'Yes') >> trigger_owner_invoice_sync >> wait_for_invoice_dags >> has_invalid_invoices
        has_valid_invoices >> rail.Label('No') >> has_invalid_invoices

        has_invalid_invoices >> rail.Label(
            'Yes') >> write_log_invoice_items_not_found >> catch_error
        has_invalid_invoices >> rail.Label('No') >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
