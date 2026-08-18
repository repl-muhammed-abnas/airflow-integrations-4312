import rail
import hashlib
import json
from datetime import datetime, timedelta, timezone
from ce_procore_integration.ap_invoice_payment_sync.utils.constants import (
    InputSource,
    CE_FIELDS
)
from ce_procore_integration.ap_invoice_payment_sync.utils.util import (
    convert_date,
    clean_currency,
    build_unique_key,
    parse_unique_key
)
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore AP Invoice Payment Sync - Delta Detection',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval='0 0 * * *' if config.input_source == InputSource.EMAIL
            else timedelta(minutes=config.schedule_interval_minutes),
        default_args={
            'imap_conn_id': config.imap_conn_id,
            'sftp_conn_id': config.sftp_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"


        if config.input_source == InputSource.EMAIL:
            def extract_csv_from_email(response):
                """Extract CSV/Excel attachment from email response."""
                if not response:
                    return {
                        'csv_file': '',
                        'csv_file_artifact': '',
                        'is_file_present': False
                    }

                csv_file = ''
                csv_file_artifact = ''

                for email in response:
                    if email.get('attachments'):
                        for attachment in email['attachments']:
                            filename = attachment['filename']

                            if filename == f'{config.ap_invoice_payment_report_filename}.csv':
                                csv_file = filename
                                csv_file_artifact = attachment['artifact']
                                break
                    if csv_file_artifact:
                        break

                return {
                    'csv_file': csv_file,
                    'csv_file_artifact': csv_file_artifact,
                    'is_file_present': bool(csv_file_artifact)
                }

            read_emails_from_inbox = rail.ReadEmailOperator(
                task_id='read_emails_from_inbox',
                subject_pattern=config.email_subject_pattern,
                limit=config.email_limit,
                max_emails_to_check=config.max_emails_to_check,
                data_handler=extract_csv_from_email
            )

            is_email_found_with_file = rail.IfOperator(
                task_id='is_email_found_with_file',
                test=lambda: (
                    rail.result('read_emails_from_inbox') and
                    rail.result('read_emails_from_inbox').get('is_file_present', False)
                ),
                yes_task='batch_task',
                no_task='send_missing_file_notification'
            )

            send_missing_file_notification = rail.EmailOperator(
                task_id='send_missing_file_notification',
                to=get_tenant_email(config),
                bcc=config.internal_email,
                subject='Computerease-Procore Integration: AP Invoice Payment Sync - No File Found - {{ current_time() }}',
                html_content='/email_templates/ap_invoice_payment_missing_file.html'
            )

        else:
            new_file_sensor = rail.SFTPAnyFileSensor(
                task_id='new_file_sensor',
                path=config.file_path,
                soft_fail_timeout=timedelta(minutes=config.sftp_sensor_timeout_minutes)
            )

            download_artifact = rail.SFTPDownloadFileOperator(
                task_id='download_artifact',
                remote_filepath="{{ result('new_file_sensor') }}"
            )

            is_new_file_found = rail.IfOperator(
                task_id='is_new_file_found',
                trigger_rule='all_done',
                test='{{ get_task_state("new_file_sensor") == "success" }}',
                yes_task='archive_file',
                no_task='delete_this_dagrun'
            )

            archive_file = rail.SFTPMoveFileOperator(
                task_id='archive_file',
                existing_filename="{{ result('new_file_sensor') }}",
                new_filename=config.archive_file_path +
                    "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
            )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_csv',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Conditional document source based on input type
        csv_document = ("{{ result('read_emails_from_inbox').csv_file_artifact }}"
            if config.input_source == InputSource.EMAIL
            else "{{ result('download_artifact') }}")
        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document=csv_document
        )

        def parse_ap_invoice_payment_data():
            ap_invoice_payments = []
            csv_data = rail.load_all_records(rail.result('load_csv'))

            for item in csv_data:
                record = {
                    'job_number': item.get(CE_FIELDS.JOB_NUMBER, ''),
                    'po_number': item.get(CE_FIELDS.PO_NUMBER, ''),
                    'date': convert_date(item.get(CE_FIELDS.DATE, '')),
                    'check_number': item.get(CE_FIELDS.CHECK_NUMBER, ''),
                    'voucher_number': item.get(CE_FIELDS.VOUCHER_NUMBER, ''),
                    'invoice_number': item.get(CE_FIELDS.INVOICE_NUMBER, ''),
                    'paid': clean_currency(item.get(CE_FIELDS.PAID, '0.00')),
                    'discount': clean_currency(item.get(CE_FIELDS.DISCOUNT, '0.00'))
                }
                if record['check_number'] and record['voucher_number']:
                    ap_invoice_payments.append(record)

            return ap_invoice_payments

        parse_invoice_data = rail.PythonOperator(
            task_id='parse_invoice_data',
            python_callable=parse_ap_invoice_payment_data
        )

        def aggregate_by_check():
            """Group payment records by check and aggregate amounts."""
            invoice_records = rail.result('parse_invoice_data')

            check_groups = {}
            for record in invoice_records:
                key = f"{record['check_number']}_{record['voucher_number']}"

                if key not in check_groups:
                    check_groups[key] = record.copy()
                else:
                    check_groups[key]['paid'] = str(
                        float(check_groups[key]['paid']) + float(record['paid'])
                    )
                    check_groups[key]['discount'] = str(
                        float(check_groups[key]['discount']) + float(record['discount'])
                    )

            return list(check_groups.values())


        EXHAUSTED_SUFFIX = '_EXHAUSTED'

        def extract_base_fingerprint(fingerprint):
            # Strip any retry/terminal suffix to recover the raw data hash
            for marker in ('_FAILED_', EXHAUSTED_SUFFIX):
                if marker in fingerprint:
                    return fingerprint.split(marker)[0]
            return fingerprint

        def is_retry_time_reached(previous_fp):
            # Return True once the backoff window recorded in the fingerprint has elapsed
            try:
                retry_time = datetime.fromisoformat(
                    previous_fp.split('_FAILED_')[1].split('_RETRYAFTER_')[1])
                retry_buffer = timedelta(minutes=config.retry_buffer_minutes)
                return datetime.now(timezone.utc) >= (retry_time - retry_buffer)
            except Exception as error:
                # Malformed marker (missing/invalid retry time) -> fail open and allow the retry
                print(f"is_retry_time_reached: unparseable fingerprint {previous_fp!r}: {error}")
                return True

        def next_failure_count(previous_value, reset):
            # A hash-only fingerprint (legacy, no suffix) or a reset (data change / force retry)
            # starts the count at 1 -> first failure gets _FAILED_1_RETRYAFTER_. A _FAILED_ marker
            # is always written together with _RETRYAFTER_, so its count is parseable -> increment.
            if '_FAILED_' in previous_value and not reset:
                return int(previous_value.split('_FAILED_')[1].split('_RETRYAFTER_')[0]) + 1
            return 1

        def build_failure_fingerprint(base_fingerprint, count, dag_start_time):
            retry_delays = config.retry_delays_hours
            delay_hours = retry_delays[count - 1] if count <= len(retry_delays) else retry_delays[-1]
            next_retry_time = dag_start_time + timedelta(hours=delay_hours)
            return f"{base_fingerprint}_FAILED_{count}_RETRYAFTER_{next_retry_time.isoformat()}"

        def get_force_retry_keys(dag_run):
            keys = dag_run.conf.get('force_retry_keys', []) if dag_run and dag_run.conf else []
            if isinstance(keys, str):
                keys = [keys]
            return keys or []

        def collect_failed_unique_keys():
            """Union of unique_keys that failed this run (child/payment DAG failures + exceptions)."""
            failed_unique_keys = set()

            # Failures gathered from child and payment DAGs (guard against skipped upstreams)
            try:
                child_dag_failures = rail.result('gather_child_dag_failures') or []
            except Exception:
                child_dag_failures = []
            try:
                child_dag_failures = child_dag_failures + (rail.result('gather_payment_dag_failures') or [])
            except Exception:
                pass
            for child_result in child_dag_failures:
                if child_result:  # each entry is a list of unique_keys
                    for unique_key in child_result:
                        failed_unique_keys.add(unique_key)

            # Invalid payments never triggered a child DAG but are still failures
            company_id = rail.render_template(procore_company_id_template)
            invalid_payments = rail.result('group_payments_by_job').get('invalid', [])
            for payment in invalid_payments:
                failed_unique_keys.add(
                    build_unique_key(company_id, payment['check_number'], payment['voucher_number'])
                )

            return failed_unique_keys

        def get_unique_key_from_log(entry):
            return entry.get('properties', {}).get('unique_key') or None

        def create_fingerprints():
            company_id = rail.render_template(procore_company_id_template)
            aggregated_records = aggregate_by_check()
            fingerprints = []

            for record in aggregated_records:
                unique_key = build_unique_key(
                    company_id,
                    record['check_number'],
                    record['voucher_number']
                )

                # Create fingerprint from ALL fields (sorted for consistency)
                fingerprint = hashlib.md5(
                    json.dumps(record, sort_keys=True).encode()
                ).hexdigest()

                fingerprints.append({
                    'unique_key': unique_key,
                    'fingerprint': fingerprint,
                    'original_data': json.dumps(record)
                })

            return fingerprints

        generate_fingerprints = rail.PythonOperator(
            task_id='generate_fingerprints',
            python_callable=create_fingerprints
        )

        search_fingerprint_file_in_s3 = rail.S3ListKeysOperator(
            task_id='search_fingerprint_file_in_s3',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            prefix=config.s3_fingerprints_prefix
        )

        if_previous_fingerprints_found = rail.IfOperator(
            task_id='if_previous_fingerprints_found',
            test='{{ result("search_fingerprint_file_in_s3") | length > 0 and "' +
                config.s3_fingerprints_key + '" in result("search_fingerprint_file_in_s3") }}',
            yes_task='download_previous_fingerprints',
            no_task='compare_fingerprints'
        )

        download_previous_fingerprints = rail.S3DownloadFileOperator(
            task_id='download_previous_fingerprints',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_fingerprints_key
        )

        load_previous_fingerprints = rail.LoadCSVFileOperator(
            task_id='load_previous_fingerprints',
            document="{{ result('download_previous_fingerprints') }}"
        )

        def compare_and_detect_changes(dag_run):
            """Compare current fingerprints with previous to detect changes (backoff-aware)."""
            current_fingerprints = rail.result('generate_fingerprints')
            force_retry_keys = get_force_retry_keys(dag_run)

            # Check if previous fingerprints exist
            previous_fingerprints_exist = (
                len(rail.result('search_fingerprint_file_in_s3')) > 0 and
                config.s3_fingerprints_key in rail.result('search_fingerprint_file_in_s3')
            )

            new_records = []
            changed_records = []
            unchanged_records = []
            removed_records = []

            if previous_fingerprints_exist:
                previous_fps = rail.load_all_records(rail.result('load_previous_fingerprints'))
                previous_fp_dict = {fp['unique_key']: fp for fp in previous_fps}
                current_fp_dict = {fp['unique_key']: fp for fp in current_fingerprints}

                # Detect new and changed records
                for current_fp in current_fingerprints:
                    unique_key = current_fp['unique_key']
                    previous_fp = previous_fp_dict.get(unique_key)

                    if previous_fp is None:
                        # New record
                        new_records.append({
                            'unique_key': unique_key,
                            'status': 'NEW',
                            'data': json.loads(current_fp['original_data'])
                        })
                    else:
                        previous_value = previous_fp['fingerprint']
                        previous_base = extract_base_fingerprint(previous_value)
                        is_failed = '_FAILED_' in previous_value
                        is_exhausted = EXHAUSTED_SUFFIX in previous_value
                        forced = unique_key in force_retry_keys

                        if current_fp['fingerprint'] != previous_base:
                            # Underlying data changed -> reprocess as a fresh attempt
                            reprocess = True
                        elif forced and (is_failed or is_exhausted):
                            # Manual override revives a failed/exhausted payment
                            reprocess = True
                        elif is_exhausted:
                            # Terminal state, data unchanged -> never retry
                            reprocess = False
                        elif is_failed and is_retry_time_reached(previous_value):
                            # Backoff window elapsed -> retry
                            reprocess = True
                        else:
                            # Clean & unchanged, or still within the backoff window
                            reprocess = False

                        if reprocess:
                            changed_records.append({
                                'unique_key': unique_key,
                                'status': 'CHANGED',
                                'current_data': json.loads(current_fp['original_data']),
                                'previous_data': json.loads(previous_fp.get('original_data', '{}'))
                            })
                        else:
                            unchanged_records.append({
                                'unique_key': unique_key,
                                'status': 'UNCHANGED'
                            })

                # Detect removed records (backoff-aware, mirroring the changed-record logic:
                # a failed deletion is retried on its own backoff schedule and stops once exhausted)
                for prev_unique_key in previous_fp_dict.keys():
                    if prev_unique_key not in current_fp_dict:
                        previous_value = previous_fp_dict[prev_unique_key]['fingerprint']
                        is_failed = '_FAILED_' in previous_value
                        is_exhausted = EXHAUSTED_SUFFIX in previous_value
                        forced = prev_unique_key in force_retry_keys

                        if forced and (is_failed or is_exhausted):
                            # Manual override revives a failed/exhausted deletion
                            attempt_removal = True
                        elif is_exhausted:
                            # Terminal state -> never retry the deletion
                            attempt_removal = False
                        elif is_failed and not is_retry_time_reached(previous_value):
                            # Still within the backoff window -> wait
                            attempt_removal = False
                        else:
                            # Clean removal, or backoff window elapsed -> attempt the deletion
                            attempt_removal = True

                        if attempt_removal:
                            removed_records.append({
                                'unique_key': prev_unique_key,
                                'status': 'REMOVED',
                                'previous_data': json.loads(previous_fp_dict[prev_unique_key].get('original_data', '{}'))
                            })
            else:
                # First run - all records are new
                new_records = [
                    {
                        'unique_key': fp['unique_key'],
                        'status': 'NEW',
                        'data': json.loads(fp['original_data'])
                    } for fp in current_fingerprints
                ]

            return {
                'new_records': new_records,
                'changed_records': changed_records,
                'unchanged_records': unchanged_records,
                'removed_records': removed_records,
                'summary': {
                    'total_current': len(current_fingerprints),
                    'new_count': len(new_records),
                    'changed_count': len(changed_records),
                    'unchanged_count': len(unchanged_records),
                    'removed_count': len(removed_records)
                }
            }

        compare_fingerprints = rail.PythonOperator(
            task_id='compare_fingerprints',
            python_callable=compare_and_detect_changes
        )


        fetch_procore_projects = rail.ProcoreApiOperator(
            task_id='fetch_procore_projects',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda projects: {
                x['origin_id']: x['id'] for x in projects if x['origin_id']
            } if projects else {}
        )

        def aggregate_payments_by_job():
            delta_results = rail.result('compare_fingerprints')
            new_records = delta_results.get('new_records', [])
            changed_records = delta_results.get('changed_records', [])
            removed_records = delta_results.get('removed_records', [])

            procore_projects = rail.result('fetch_procore_projects')

            job_groups = {}
            invalid_payments = []

            for record in new_records + changed_records:
                data = record.get('data') or record.get('current_data')
                job_number = data['job_number']
                project_id = procore_projects.get(f'CE_{job_number}')

                if not project_id:
                    invalid_payments.append({
                        'job_number': job_number,
                        'check_number': data['check_number'],
                        'voucher_number': data['voucher_number'],
                        'reason': 'Project not found in Procore'
                    })
                    continue

                if job_number not in job_groups:
                    job_groups[job_number] = {
                        'new': [],
                        'changed': [],
                        'removed': [],
                        'project_id': project_id
                    }

                if record in new_records:
                    job_groups[job_number]['new'].append(data)
                else:
                    job_groups[job_number]['changed'].append(data)

            for record in removed_records:
                prev_data = record.get('previous_data')
                job_number = prev_data['job_number']
                project_id = procore_projects.get(f'CE_{job_number}')

                if not project_id:
                    invalid_payments.append({
                        'job_number': job_number,
                        'voucher_number': prev_data['voucher_number'],
                        'check_number': prev_data['check_number'],
                        'reason': 'Project not found in Procore'
                    })
                    continue

                if job_number not in job_groups:
                    job_groups[job_number] = {
                        'new': [],
                        'changed': [],
                        'removed': [],
                        'project_id': project_id
                    }

                job_groups[job_number]['removed'].append(prev_data)

            batches = [{'job_number': k, **v} for k, v in job_groups.items()]
            return {'batches': batches, 'invalid': invalid_payments}

        group_payments_by_job = rail.PythonOperator(
            task_id='group_payments_by_job',
            python_callable=aggregate_payments_by_job
        )

        has_payment_exceptions = rail.IfOperator(
            task_id='has_payment_exceptions',
            test=lambda: len(rail.result('group_payments_by_job').get('invalid', [])) > 0,
            yes_task='write_exception',
            no_task='check_has_batches'
        )

        write_exception = rail.WriteLogOperator(
            task_id='write_exception',
            message='Payment Exception',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda: [
                {
                    'code': item.get('job_number', 'unknown'),
                    'job_code': item.get('job_number', 'unknown'),
                    'company_id': rail.render_template(procore_company_id_template),
                    'unique_key': build_unique_key(
                        rail.render_template(procore_company_id_template),
                        item['check_number'],
                        item['voucher_number']
                    ),
                    'status': 'Exception',
                    'reason': item.get('reason', 'Unknown error')
                } for item in rail.result('group_payments_by_job')['invalid']
            ]
        )

        check_has_batches = rail.IfOperator(
            task_id='check_has_batches',
            test='{{ result("group_payments_by_job").batches | length > 0 }}',
            yes_task='trigger_payment_sync',
            no_task='merge_fingerprints_excluding_failures'
        )

        trigger_payment_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_payment_sync',
            items=lambda: rail.result('group_payments_by_job')['batches'],
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'batch': item,
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_payment_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_payment_sync',
            dag_runs='{{ result("trigger_payment_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_child_dag_failures = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_dag_failures',
            dagrun_task_id='collect_child_failures',
            dag_runs='{{ result("trigger_payment_sync") }}'
        )

        gather_payment_dag_failures = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_payment_dag_failures',
            dagrun_task_id='collect_payment_failures',
            dag_runs='{{ result("trigger_payment_sync") }}'
        )

        def get_merged_fingerprints_excluding_failures(dag_run):
            """Merge fingerprints and classify failures for backoff retry.

            Failed payments get a `_FAILED_<count>_RETRYAFTER_<time>` marker (exponential
            backoff); once retries are exhausted the fingerprint becomes terminal
            (`<hash>_EXHAUSTED`). Returns the merged list plus the keys that failed for the
            first time or reached exhaustion this run (used to gate email alerts).
            """
            failed_unique_keys = collect_failed_unique_keys()
            force_retry_keys = set(get_force_retry_keys(dag_run))
            dag_start_time = datetime.now(timezone.utc)
            max_retries = len(config.retry_delays_hours)

            # Records actually processed this run (delta sent downstream)
            delta_results = rail.result('compare_fingerprints')
            processed_this_run = {
                record['unique_key']
                for record in delta_results.get('new_records', []) + delta_results.get('changed_records', [])
            }

            # Successfully removed records should drop out of the fingerprint file
            successfully_removed_keys = {
                record['unique_key']
                for record in delta_results.get('removed_records', [])
                if record['unique_key'] not in failed_unique_keys
            }

            current_fingerprints = rail.result('generate_fingerprints')
            previous_fps_exist = (
                len(rail.result('search_fingerprint_file_in_s3')) > 0 and
                config.s3_fingerprints_key in rail.result('search_fingerprint_file_in_s3')
            )
            if previous_fps_exist:
                previous_fps_records = rail.load_all_records(rail.result('load_previous_fingerprints'))
                previous_fps_dict = {fp['unique_key']: fp for fp in previous_fps_records}
            else:
                previous_fps_dict = {}

            merged_fingerprints = []
            first_failures = []
            exhausted = []
            processed_keys = set()

            def apply_failure_state(base_fingerprint, previous_value, reset, unique_key):
                """Advance the backoff state for a failed key: return the marked fingerprint
                and record it in first_failures / exhausted so the alert email can be gated."""
                count = next_failure_count(previous_value, reset)
                if count > max_retries:
                    # Retries exhausted -> terminal marker, no further automatic retries
                    exhausted.append(unique_key)
                    return f"{base_fingerprint}{EXHAUSTED_SUFFIX}"
                if count == 1:
                    first_failures.append(unique_key)
                return build_failure_fingerprint(base_fingerprint, count, dag_start_time)

            for current_fp in current_fingerprints:
                unique_key = current_fp['unique_key']
                processed_keys.add(unique_key)
                previous_value = previous_fps_dict.get(unique_key, {}).get('fingerprint', '')
                base_fp = {
                    'unique_key': unique_key,
                    'original_data': current_fp['original_data']
                }

                if unique_key in failed_unique_keys:
                    # Reset the backoff when the data changed or a manual retry was requested
                    reset = (extract_base_fingerprint(previous_value) != current_fp['fingerprint']
                             or unique_key in force_retry_keys)
                    base_fp['fingerprint'] = apply_failure_state(
                        current_fp['fingerprint'], previous_value, reset, unique_key)
                    merged_fingerprints.append(base_fp)
                elif (unique_key not in processed_this_run and previous_value
                      and extract_base_fingerprint(previous_value) == current_fp['fingerprint']):
                    # Not processed this run and data unchanged -> keep prior state
                    # (preserves an in-backoff FAILED / terminal EXHAUSTED marker)
                    merged_fingerprints.append(previous_fps_dict[unique_key])
                else:
                    # Succeeded this run (or clean & unchanged) -> store clean fingerprint
                    merged_fingerprints.append(current_fp)

            # A failed deletion is not in current_fingerprints, so give it the same backoff
            # treatment here: mark it FAILED/EXHAUSTED, surface it for alerting, and persist the
            # marker so compare_fingerprints can gate future retry attempts and eventually stop.
            handled_removed = set()
            for record in delta_results.get('removed_records', []):
                key = record['unique_key']
                if key in failed_unique_keys:
                    prev = previous_fps_dict.get(key, {})
                    previous_value = prev.get('fingerprint', '')
                    base_fingerprint = extract_base_fingerprint(previous_value)
                    merged_fingerprints.append({
                        'unique_key': key,
                        'original_data': prev.get(
                            'original_data', json.dumps(record.get('previous_data', {}))),
                        'fingerprint': apply_failure_state(
                            base_fingerprint, previous_value, key in force_retry_keys, key)
                    })
                    handled_removed.add(key)

            # Re-add previous fingerprints not present in the current run, except records that
            # were successfully removed or already handled as failed removals above.
            for prev_key, prev_fp in previous_fps_dict.items():
                if (prev_key not in processed_keys
                        and prev_key not in successfully_removed_keys
                        and prev_key not in handled_removed):
                    merged_fingerprints.append(prev_fp)

            return {
                'fingerprints': merged_fingerprints,
                'first_failures': first_failures,
                'exhausted': exhausted
            }

        merge_fingerprints_excluding_failures = rail.PythonOperator(
            task_id='merge_fingerprints_excluding_failures',
            trigger_rule='none_failed_min_one_success',
            python_callable=get_merged_fingerprints_excluding_failures
        )

        write_current_fingerprints_csv = rail.WriteCSVFileOperator(
            task_id='write_current_fingerprints_csv',
            source="{{ result('merge_fingerprints_excluding_failures')['fingerprints'] | to_json }}",
            header=['unique_key', 'fingerprint', 'original_data'],
            row=[
                "{{ item.unique_key }}",
                "{{ item.fingerprint }}",
                "{{ item.original_data }}"
            ]
        )

        upload_fingerprints_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_fingerprints_to_s3',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('write_current_fingerprints_csv') }}",
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_fingerprints_key,
            replace=True
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        def build_failure_report():
            """Rows for the alert email: first-failure and exhausted payments, plus any
            errors that could not be attributed to a specific payment."""
            notifications = rail.result('merge_fingerprints_excluding_failures')
            first_failures = set(notifications.get('first_failures', []))
            exhausted = set(notifications.get('exhausted', []))
            max_retries = len(config.retry_delays_hours)

            # Correlate per-payment details (reason/job_code/ecid) from payment-level logs
            # (unique_key property) + the structured invalid list, so each report row can be
            # traced back to its log entry.
            detail_by_key = {}
            uncorrelated = []
            for entry in rail.load_all_records(rail.result('search_logs')):
                key = get_unique_key_from_log(entry)
                if key:
                    props = entry.get('properties', {})
                    detail_by_key.setdefault(key, {
                        'reason': props.get('reason', ''),
                        'job_code': props.get('job_code', ''),
                        'ecid': entry.get('ecid', '')
                    })
                else:
                    uncorrelated.append(entry)

            company_id = rail.render_template(procore_company_id_template)
            for payment in rail.result('group_payments_by_job').get('invalid', []):
                key = build_unique_key(company_id, payment['check_number'], payment['voucher_number'])
                detail_by_key.setdefault(key, {
                    'reason': payment.get('reason', ''),
                    'job_code': payment.get('job_number', ''),
                    'ecid': ''
                })

            def split_key(unique_key):
                company, check, voucher = parse_unique_key(unique_key)
                return (check, voucher)

            rows = []
            for key in sorted(first_failures):
                check_number, voucher_number = split_key(key)
                detail = detail_by_key.get(key, {})
                rows.append({
                    'check_number': check_number,
                    'voucher_number': voucher_number,
                    'status': 'Error/Exception',
                    'reason': detail.get('reason') or 'See attached logs',
                    'company_id': company_id,
                    'job_code': detail.get('job_code', ''),
                    'ecid': detail.get('ecid', '')
                })
            for key in sorted(exhausted):
                check_number, voucher_number = split_key(key)
                detail = detail_by_key.get(key, {})
                base_reason = detail.get('reason') or 'See attached logs'
                rows.append({
                    'check_number': check_number,
                    'voucher_number': voucher_number,
                    'status': 'Error/Exception',
                    'reason': (f"{base_reason} | Retry attempts exhausted after "
                               f"{max_retries} retries; no further automatic retries."),
                    'company_id': company_id,
                    'job_code': detail.get('job_code', ''),
                    'ecid': detail.get('ecid', '')
                })
            for entry in uncorrelated:
                properties = entry.get('properties', {})
                rows.append({
                    'unique_key': properties.get('code', ''),
                    'check_number': '',
                    'voucher_number': '',
                    'status': properties.get('status', ''),
                    'reason': properties.get('reason', ''),
                    'company_id': properties.get('company_id', ''),
                    'job_code': properties.get('job_code', ''),
                    'ecid': entry.get('ecid', '')
                })
            return rows

        build_failure_report_task = rail.PythonOperator(
            task_id='build_failure_report',
            python_callable=build_failure_report
        )

        if_send_failure_email = rail.IfOperator(
            task_id='if_send_failure_email',
            test='{{ result("build_failure_report") | length > 0 }}',
            yes_task='write_logs_into_csv',
            no_task='log_to_sumo'
        )

        write_logs_into_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_into_csv',
            source='{{ result("build_failure_report") | to_json }}',
            header=['Company Id', 'Job Code', 'Voucher Number',
                'Check Number', 'Status', 'Reason', 'ECID'],
            row=[
                "{{ item.company_id }}",
                "{{ item.job_code }}",
                "{{ item.voucher_number }}",
                "{{ item.check_number }}",
                "{{ item.status }}",
                "{{ item.reason }}",
                "{{ item.ecid }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_APInvoicePaymentSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: AP Invoice Payment Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/ap_invoice_payment_sync_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        if config.input_source == InputSource.EMAIL:
            read_emails_from_inbox >> is_email_found_with_file
            is_email_found_with_file >> rail.Label('Yes') >> batch_task
            is_email_found_with_file >> rail.Label('No') >> send_missing_file_notification >> delete_this_dagrun
        else:
            new_file_sensor >> download_artifact >> batch_task
            download_artifact >> rail.Label('Always') >> is_new_file_found
            is_new_file_found >> rail.Label('Yes') >> archive_file
            is_new_file_found >> rail.Label('No') >> delete_this_dagrun

        batch_task >> log_to_sumo
        batch_task >> load_csv >> parse_invoice_data >> generate_fingerprints
        generate_fingerprints >> search_fingerprint_file_in_s3 >> if_previous_fingerprints_found

        if_previous_fingerprints_found >> rail.Label('Yes') >> download_previous_fingerprints >> load_previous_fingerprints >> compare_fingerprints
        if_previous_fingerprints_found >> rail.Label('No') >> compare_fingerprints
        compare_fingerprints >> fetch_procore_projects >> group_payments_by_job >> has_payment_exceptions

        has_payment_exceptions >> rail.Label('Yes') >> write_exception >> check_has_batches
        has_payment_exceptions >> rail.Label('No') >> check_has_batches

        check_has_batches >> rail.Label('Yes') >> trigger_payment_sync >> wait_for_payment_sync >> gather_child_dag_failures >> gather_payment_dag_failures
        gather_payment_dag_failures >> merge_fingerprints_excluding_failures
        check_has_batches >> rail.Label('No') >> merge_fingerprints_excluding_failures

        merge_fingerprints_excluding_failures >> write_current_fingerprints_csv >> upload_fingerprints_to_s3 >> search_logs
        search_logs >> build_failure_report_task >> if_send_failure_email

        if_send_failure_email >> rail.Label('Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_send_failure_email >> rail.Label('No') >> log_to_sumo

        return dag

rail.for_each_instance(create_dag_instance)
