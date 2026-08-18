"""
Azenta Polaris → Oracle PPM Time Export Integration (FI017)
Exports approved worked-time entries from Replicon Polaris to Oracle Fusion Cloud PPM project costs.

Flow:
  [can_run_batch_task] → BatchTaskRunOperator (re-run gate; runs the whole chain below in-process,
    start_task='logging_details' through end_task='should_fail_dag', when the killswitch Variable is on)
    → logging_details → get_time_download_script
    → time_data_export TaskGroup (extract → mark complete → download → load)
    → create_timeexport_collection → finish_oracle_export_batch
    → has_data
      Yes → can_post_to_oracle
            Yes → filter_eligible_records (project status + accounting cutoff)
                  → has_eligible_records
                    Yes → compute_batch_name → get_user_details → build_user_uri_map
                          → get_primary_function_roles → build_login_to_function_map
                          → has_resolvable_primary_functions
                            Yes → build_generic_resource_query_task → get_generic_resource_persons
                                  → build_person_number_map
                                  → build_oracle_rows → has_rows_to_post
                                    Yes → build_validate_envelope → submit_validate_soap
                                          → check_validation_failure
                                            Yes → log_validation_failure → build_validation_report_rows (errors only)
                                                  → render_validation_report_csv → generate_validation_report_link
                                                  → upload_validation_report_csv_sftp → send_validation_failure_email
                                                  → cancel_export_validation_failure (revert batch: draft → cancelled)
                                            No  → build_bulk_soap_envelope → submit_bulk_soap
                                                  → check_soap_fault
                                                    Yes → log_posting_failure → build_posting_report_rows
                                                          → render_posting_report_csv → generate_posting_report_link
                                                          → upload_posting_report_csv_sftp → send_posting_failure_email
                                                    No  → build_success_report_rows → render_success_report_csv
                                                          → generate_success_report_link → upload_success_report_csv_sftp
                                                          → send_success_email
                                    No  → log_no_rows_to_post
                            No  → log_no_resolvable_functions
                    No  → log_no_eligible_records
            No  → skip_oracle_posting_killswitch
      No  → update_export_name_to_no_data → send_empty_export_email
    → should_fail_dag → fail/finish
  Error path: mark_timedata_export_error → get_export_uri_failed → cancel_time_export → update_export_name_cancelled

  Note: this integration's job ends at pre-validating (validateTimecardTransaction) then posting
  (receiveTimecardTransaction) and checking that response. Triggering Oracle's separate "Import Costs"
  ESS job (which turns interfaced rows into Project Costs) is out of scope — handled independently by
  the client/Oracle-side team. Duplicate-prevention relies solely on Replicon's own
  time-data-export-status marking (already-exported records are excluded from the next export's
  filter), not an idempotency check against Oracle. On validateTimecardTransaction failure, none of
  the batch's rows were ever posted to Oracle — cancel_export_validation_failure reverts the export
  batch (draft → cancelled) so its entries fall back out of "exported" state and are re-attempted by
  the next run's filter, rather than being silently lost.

  Note: build_bulk_soap_envelope/build_validate_envelope never embed the real Oracle password —
  their XCom'd return value carries WSSE_HEADER_PLACEHOLDER instead. submit_bulk_soap/
  submit_validate_soap's `data=` substitutes the real WS-Security header in at task-render time via
  conn.get(oracle_http_conn_id) | wsse_header (the `wsse_header` Jinja filter registered below),
  so the password is never persisted to the metadata database in cleartext.
"""
from datetime import timedelta
from pendulum import datetime
import rail
from airflow.models import Variable

from azenta.time_export.tasks.time_data_export import time_data_export
from azenta.time_export.tasks.update_export_status import cancel_time_export
from azenta.time_export.utils import custom_methods
from azenta.time_export.utils import request_payload

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.dag_id,
        description=f'Azenta Polaris to Oracle PPM Time Export (FI017) — {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 8, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run,
        # Lets submit_bulk_soap/submit_validate_soap's `data=` inject the real WS-Security header
        # (via conn.get(...) | wsse_header) at task-render time, so the Oracle password never
        # passes through a PythonOperator return value / XCom — see WSSE_HEADER_PLACEHOLDER.
        user_defined_filters={'wsse_header': custom_methods.wsse_security_header_for_connection},
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'{config.can_run_batch_task_var_name}_{config.instance}',
                default_var='true'
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='logging_details',
            end_task='should_fail_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.timezone, config.export_file_prefix, config.accounting_cutoff_hour]
        )

        get_time_download_script = rail.RepliconServiceOperator(
            task_id='get_time_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'],
                'displayText', config.time_export_file_format, 'uri'
            )
        )

        group_id = 'time_data_export'

        time_export_batch_start, time_export_batch_end = time_data_export(
            group_id=group_id,
            get_export_name="{{ result('logging_details').time_export_filename }}",
            approval_filter_mode=config.time_export_approval_filter_mode
        )

        create_timeexport_collection = rail.CreateCollectionOperator(
            task_id='create_timeexport_collection',
            name='oracle_time_export_data',
            source="{{ result('" + group_id + ".load_export') }}",
            # Default trigger_rule (all_success) would mark this upstream_failed/skipped when an
            # extraction task fails, so it — and check_extraction_error right after it — would
            # never run, leaving get_failed_upstream_task_ids() unevaluated and the failed batch
            # never reaching mark_timedata_export_error/cancel_time_export.
            trigger_rule='all_done'
        )

        check_extraction_error = rail.IfOperator(
            task_id='check_extraction_error',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='mark_timedata_export_error',
            no_task='finish_oracle_export_batch'
        )

        finish_oracle_export_batch = rail.EmptyOperator(
            task_id='finish_oracle_export_batch'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test="{{ result('create_timeexport_collection', 'length') > 0 }}",
            yes_task='can_post_to_oracle',
            no_task='update_export_name_to_no_data'
        )

        can_post_to_oracle = rail.IfOperator(
            task_id='can_post_to_oracle',
            test=lambda: Variable.get(
                f'{config.can_post_to_oracle_var_name}_{config.instance}',
                default_var='true'
            ).lower() == 'true',
            yes_task='filter_eligible_records',
            no_task='skip_oracle_posting_killswitch'
        )

        skip_oracle_posting_killswitch = rail.WriteLogOperator(
            task_id='skip_oracle_posting_killswitch',
            severity='Warning',
            message=f'{config.can_post_to_oracle_var_name}_{config.instance} kill-switch Variable '
                     'is off — skipping Oracle posting for this batch.'
        )

        filter_eligible_records = rail.PythonOperator(
            task_id='filter_eligible_records',
            python_callable=lambda: custom_methods.filter_by_eligibility(
                records=rail.result('create_timeexport_collection'),
                eligible_project_statuses=config.eligible_project_statuses,
                timezone=config.timezone,
                accounting_cutoff_hour=config.accounting_cutoff_hour
            )
        )

        has_eligible_records = rail.IfOperator(
            task_id='has_eligible_records',
            test="{{ result('filter_eligible_records') | length > 0 }}",
            yes_task='compute_batch_name',
            no_task='log_no_eligible_records'
        )

        log_no_eligible_records = rail.WriteLogOperator(
            task_id='log_no_eligible_records',
            severity='Warning',
            message="{{ result('create_timeexport_collection', 'length') }} extracted record(s) had "
                    "none eligible (project status/accounting-cutoff filtered) — skipping Oracle "
                    "posting for this batch."
        )

        # Single BatchName shared by every row in this bulk submission.
        compute_batch_name = rail.PythonOperator(
            task_id='compute_batch_name',
            python_callable=lambda: custom_methods.compute_batch_name(
                rail.result('filter_eligible_records'), config.timezone
            )
        )

        # Client-confirmed: Primary Function comes from Replicon's current effective project-role
        # assignment (isPrimary=true), not a report column. Resolved once per batch for all distinct
        # logins, not per loop iteration.
        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: request_payload.build_bulk_get_users_request(
                custom_methods.get_distinct_logins(rail.result('filter_eligible_records'))
            )
        )

        build_user_uri_map = rail.PythonOperator(
            task_id='build_user_uri_map',
            python_callable=lambda: custom_methods.build_login_to_user_uri_map(
                custom_methods.get_distinct_logins(rail.result('filter_eligible_records')),
                rail.result('get_user_details')
            )
        )

        get_primary_function_roles = rail.RepliconServiceOperator(
            task_id='get_primary_function_roles',
            endpoint='/services/ResourceService1.svc/BulkGetProjectRoleAssignmentScheduleForUsers',
            data=lambda: request_payload.build_primary_function_roles_request(
                list(rail.result('build_user_uri_map').values())
            )
        )

        build_login_to_function_map = rail.PythonOperator(
            task_id='build_login_to_function_map',
            python_callable=lambda: custom_methods.build_login_to_function_map(
                rail.result('build_user_uri_map'),
                rail.result('get_primary_function_roles')
            )
        )

        # Empty login_to_function_map means no eligible record's login resolved to a Primary
        # Function — guard here rather than calling get_generic_resource_persons with zero
        # functions, which would otherwise send an unfiltered limit=500 GET against publicWorkers.
        has_resolvable_primary_functions = rail.IfOperator(
            task_id='has_resolvable_primary_functions',
            test="{{ result('build_login_to_function_map') | length > 0 }}",
            yes_task='build_generic_resource_query_task',
            no_task='log_no_resolvable_functions'
        )

        log_no_resolvable_functions = rail.WriteLogOperator(
            task_id='log_no_resolvable_functions',
            severity='Warning',
            message="{{ result('filter_eligible_records') | length }} eligible record(s) had no "
                    "resolvable Primary Function (build_login_to_function_map returned empty) — "
                    "skipping Oracle posting for this batch."
        )

        # Primary Function name → generic-resource {PersonNumber, PersonName}, resolved via an
        # exact DisplayName= match against Oracle HCM's publicWorkers resource.
        #
        # data must NOT be a bare lambda here: SimpleHttpOperator is stock Airflow's HttpOperator,
        # which has no callable-resolution logic, and BatchTaskRunOperator renders template fields
        # via Airflow's render_template() (not the scheduler's callable-aware _render_template_field),
        # which also has no callable branch — a lambda passes through unresolved as a raw function
        # object, which requests then feeds straight into urllib.parse.urlunparse(), raising
        # "TypeError: Cannot mix str and non-str arguments". Building the query dict in an upstream
        # PythonOperator and referencing its "q" value via Jinja keeps this templated correctly.
        build_generic_resource_query_task = rail.PythonOperator(
            task_id='build_generic_resource_query_task',
            python_callable=lambda: custom_methods.build_generic_resource_query(
                custom_methods.get_distinct_primary_functions(rail.result('build_login_to_function_map'))
            )
        )

        get_generic_resource_persons = rail.SimpleHttpOperator(
            task_id='get_generic_resource_persons',
            http_conn_id=config.oracle_http_conn_id,
            method='GET',
            endpoint=config.oracle_hcm_public_workers_path,
            data={
                "q": "{{ result('build_generic_resource_query_task')['q'] }}",
                "fields": "{{ result('build_generic_resource_query_task')['fields'] }}",
                "onlyData": "{{ result('build_generic_resource_query_task')['onlyData'] }}",
                "limit": "{{ result('build_generic_resource_query_task')['limit'] }}"
            },
            headers={"Accept": "application/json"}
        )

        build_person_number_map = rail.PythonOperator(
            task_id='build_person_number_map',
            python_callable=lambda: custom_methods.build_person_number_map(
                rail.result('get_generic_resource_persons')
            )
        )

        # Resolves every eligible record's Oracle row in one in-process pass (PersonNumber/
        # PersonName identify a generic Primary-Function resource, not the real employee).
        build_oracle_rows = rail.PythonOperator(
            task_id='build_oracle_rows',
            python_callable=lambda: custom_methods.build_oracle_rows(
                rail.result('filter_eligible_records'),
                rail.result('build_login_to_function_map'),
                rail.result('build_person_number_map'),
                rail.result('compute_batch_name'),
                config
            )
        )

        # A record's Primary Function can be resolved batch-wide (has_resolvable_primary_functions)
        # while that specific record's own login/function still fails to resolve a person — guard
        # again here so a fully-skipped batch is logged instead of validating/posting zero rows.
        has_rows_to_post = rail.IfOperator(
            task_id='has_rows_to_post',
            test="{{ result('build_oracle_rows').rows | length > 0 }}",
            yes_task='build_validate_envelope',
            no_task='log_no_rows_to_post'
        )

        log_no_rows_to_post = rail.WriteLogOperator(
            task_id='log_no_rows_to_post',
            severity='Warning',
            message=lambda: custom_methods.format_no_rows_message(rail.result('build_oracle_rows'))
        )

        build_validate_envelope = rail.PythonOperator(
            task_id='build_validate_envelope',
            python_callable=lambda: custom_methods.build_validate_soap_envelope(
                rail.result('build_oracle_rows')['rows']
            )
        )

        submit_validate_soap = rail.SimpleHttpOperator(
            task_id='submit_validate_soap',
            http_conn_id=config.oracle_http_conn_id,
            endpoint=config.oracle_soap_project_txn_path,
            method='POST',
            headers={
                'Content-Type': 'text/xml; charset=utf-8',
                'SOAPAction': config.oracle_soap_action_validate_txn
            },
            # The real WS-Security header (with the Oracle password) is injected here at
            # render time — never via build_validate_envelope's XCom'd return value.
            data=(
                "{{ result('build_validate_envelope').replace('"
                + custom_methods.WSSE_HEADER_PLACEHOLDER + "', "
                "conn.get('" + config.oracle_http_conn_id + "') | wsse_header) }}"
            ),
            extra_options={'check_response': False}
        )

        check_validation_failure = rail.IfOperator(
            task_id='check_validation_failure',
            test=lambda: custom_methods.has_validation_failure(rail.result('submit_validate_soap')),
            yes_task='format_validation_failure_message_task',
            no_task='build_bulk_soap_envelope'
        )

        format_validation_failure_message_task = rail.PythonOperator(
            task_id='format_validation_failure_message_task',
            python_callable=lambda: custom_methods.format_validation_failure_message(
                rail.result('submit_validate_soap')
            )
        )

        log_validation_failure = rail.WriteLogOperator(
            task_id='log_validation_failure',
            severity='Error',
            message="{{ result('format_validation_failure_message_task') }}"
        )

        # Only the failing rows go into the validation-failure report — rows that already passed
        # validateTimecardTransaction add nothing actionable for the client to fix.
        build_validation_report_rows = rail.PythonOperator(
            task_id='build_validation_report_rows',
            python_callable=lambda: custom_methods.filter_error_rows(
                custom_methods.build_export_report_rows(
                    rail.result('filter_eligible_records'), rail.result('build_oracle_rows'),
                    rail.result('submit_validate_soap'), config
                )
            )
        )

        render_validation_report_csv = rail.WriteCSVFileOperator(
            task_id='render_validation_report_csv',
            source=lambda: rail.result('build_validation_report_rows'),
            header=['Login', 'Project Id', 'Entry Date', 'Hours', 'Status', 'Message'],
            row=lambda item: [
                item.get('Login', ''), item.get('Project Id', ''), item.get('Entry Date', ''),
                item.get('Hours', ''), item.get('Status', ''), item.get('Message', '')
            ]
        )

        generate_validation_report_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_validation_report_link',
            artifact_name="{{ result('render_validation_report_csv') }}",
            output_file_name="{{ result('logging_details').time_export_filename }}_validation_report.csv",
            expires_in_seconds=config.report_download_link_expires_in_seconds
        )

        upload_validation_report_csv_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_validation_report_csv_sftp',
            sftp_conn_id=config.report_sftp_conn_id,
            content="{{ result('render_validation_report_csv') }}",
            remote_filepath=config.report_sftp_remote_dir + '/'
                            + "{{ result('logging_details').time_export_filename }}_validation_report.csv"
        )

        # These records were already marked exported by time_data_export (before Oracle ever saw
        # them) but never actually posted — revert the export batch to draft then cancelled so its
        # entries fall back out of "exported" state and are picked up again by the next run's filter.
        mark_export_status_cancel_validation_start, mark_export_status_cancel_validation_end = cancel_time_export(
            export_uri_task_id=f'{group_id}.get_export_uri',
            group_id='cancel_export_validation_failure'
        )

        build_bulk_soap_envelope = rail.PythonOperator(
            task_id='build_bulk_soap_envelope',
            python_callable=lambda: custom_methods.build_bulk_soap_envelope(
                rail.result('build_oracle_rows')['rows']
            )
        )

        submit_bulk_soap = rail.SimpleHttpOperator(
            task_id='submit_bulk_soap',
            http_conn_id=config.oracle_http_conn_id,
            endpoint=config.oracle_soap_project_txn_path,
            method='POST',
            headers={
                'Content-Type': 'text/xml; charset=utf-8',
                'SOAPAction': config.oracle_soap_action_project_txn
            },
            # The real WS-Security header (with the Oracle password) is injected here at
            # render time — never via build_bulk_soap_envelope's XCom'd return value.
            data=(
                "{{ result('build_bulk_soap_envelope').replace('"
                + custom_methods.WSSE_HEADER_PLACEHOLDER + "', "
                "conn.get('" + config.oracle_http_conn_id + "') | wsse_header) }}"
            ),
            # Oracle SOAP faults arrive as HTTP 500; disable check_response so the fault body reaches check_soap_fault.
            extra_options={'check_response': False},
            # Records are already marked exported upstream (time_data_export TaskGroup) and rows
            # carry financial postings — a retried-but-actually-delivered POST would double-post,
            # so fail fast on a transient error instead of retrying blind.
            retries=0
        )

        check_soap_fault = rail.IfOperator(
            task_id='check_soap_fault',
            test=lambda: custom_methods.has_soap_fault(rail.result('submit_bulk_soap')),
            yes_task='format_soap_fault_message_task',
            no_task='build_success_report_rows'
        )

        format_soap_fault_message_task = rail.PythonOperator(
            task_id='format_soap_fault_message_task',
            python_callable=lambda: custom_methods.format_soap_fault_message(rail.result('submit_bulk_soap'))
        )

        log_posting_failure = rail.WriteLogOperator(
            task_id='log_posting_failure',
            severity='Error',
            message="{{ result('format_soap_fault_message_task') }}"
        )

        build_success_report_rows = rail.PythonOperator(
            task_id='build_success_report_rows',
            python_callable=lambda: custom_methods.build_export_report_rows(
                rail.result('filter_eligible_records'), rail.result('build_oracle_rows'),
                rail.result('submit_bulk_soap'), config
            )
        )

        render_success_report_csv = rail.WriteCSVFileOperator(
            task_id='render_success_report_csv',
            source=lambda: rail.result('build_success_report_rows'),
            header=['Login', 'Project Id', 'Entry Date', 'Hours', 'Status', 'Message'],
            row=lambda item: [
                item.get('Login', ''), item.get('Project Id', ''), item.get('Entry Date', ''),
                item.get('Hours', ''), item.get('Status', ''), item.get('Message', '')
            ]
        )

        generate_success_report_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_success_report_link',
            artifact_name="{{ result('render_success_report_csv') }}",
            output_file_name="{{ result('logging_details').time_export_filename }}_success_report.csv",
            expires_in_seconds=config.report_download_link_expires_in_seconds
        )

        upload_success_report_csv_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_success_report_csv_sftp',
            sftp_conn_id=config.report_sftp_conn_id,
            content="{{ result('render_success_report_csv') }}",
            remote_filepath=config.report_sftp_remote_dir + '/'
                            + "{{ result('logging_details').time_export_filename }}_success_report.csv"
        )

        build_posting_report_rows = rail.PythonOperator(
            task_id='build_posting_report_rows',
            python_callable=lambda: custom_methods.build_export_report_rows(
                rail.result('filter_eligible_records'), rail.result('build_oracle_rows'),
                rail.result('submit_bulk_soap'), config
            )
        )

        render_posting_report_csv = rail.WriteCSVFileOperator(
            task_id='render_posting_report_csv',
            source=lambda: rail.result('build_posting_report_rows'),
            header=['Login', 'Project Id', 'Entry Date', 'Hours', 'Status', 'Message'],
            row=lambda item: [
                item.get('Login', ''), item.get('Project Id', ''), item.get('Entry Date', ''),
                item.get('Hours', ''), item.get('Status', ''), item.get('Message', '')
            ]
        )

        generate_posting_report_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_posting_report_link',
            artifact_name="{{ result('render_posting_report_csv') }}",
            output_file_name="{{ result('logging_details').time_export_filename }}_posting_report.csv",
            expires_in_seconds=config.report_download_link_expires_in_seconds
        )

        upload_posting_report_csv_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_posting_report_csv_sftp',
            sftp_conn_id=config.report_sftp_conn_id,
            content="{{ result('render_posting_report_csv') }}",
            remote_filepath=config.report_sftp_remote_dir + '/'
                            + "{{ result('logging_details').time_export_filename }}_posting_report.csv"
        )

        update_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id='update_export_name_to_no_data',
            endpoint='/services/TimeDataExportService1.svc/UpdateTimeDataExportName',
            data={
                "target": {
                    "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                },
                "name": "{{ result('logging_details').time_export_filename_nodata }}"
            }
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to Oracle — No records to export — '
                    '{{ current_time_in_specified_tz("' + config.timezone + '") }}',
            html_content='templates/emails/email_empty_export.html',
            params={'time_zone': config.timezone}
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to Oracle — Completed — '
                    '{{ current_time_in_specified_tz("' + config.timezone + '") }}',
            html_content='templates/emails/email_valid_export_complete.html',
            params={'time_zone': config.timezone}
        )

        send_posting_failure_email = rail.EmailOperator(
            task_id='send_posting_failure_email',
            to=config.tenant_email,
            bcc=[config.internal_logs_email, config.alert_email],
            subject='{{ get_company_key() }} | Replicon Time Data Export to Oracle — Posting failed — '
                    '{{ current_time_in_specified_tz("' + config.timezone + '") }}',
            html_content='templates/emails/email_posting_failure.html',
            params={'time_zone': config.timezone}
        )

        send_validation_failure_email = rail.EmailOperator(
            task_id='send_validation_failure_email',
            to=config.tenant_email,
            bcc=[config.internal_logs_email, config.alert_email],
            subject='{{ get_company_key() }} | Replicon Time Data Export to Oracle — Validation failed — '
                    '{{ current_time_in_specified_tz("' + config.timezone + '") }}',
            html_content='templates/emails/email_validation_failure.html',
            params={'time_zone': config.timezone}
        )

        # No trigger_rule override (default all_success): check_extraction_error is a
        # BaseBranchOperator subclass, so Airflow force-skips this task directly whenever the No
        # branch is chosen (regardless of trigger_rule) and leaves it eligible to run — under the
        # default all_success rule — whenever the Yes branch is chosen. all_done was wrong here:
        # it doesn't change the Yes/No force-skip behavior (that's independent of trigger_rule),
        # but it does mean this task would run on any OTHER terminal upstream state too, which is
        # unnecessary risk if this task ever gains a second upstream edge in the future.
        mark_timedata_export_error = rail.EmptyOperator(
            task_id='mark_timedata_export_error'
        )

        get_export_uri_failed = rail.RepliconServiceOperator(
            task_id='get_export_uri_failed',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + group_id + ".create_export') }}"
            },
            data_handler=request_payload.retrieve_export_uri
        )

        mark_export_status_cancel_start, mark_export_status_cancel_end = cancel_time_export(
            export_uri_task_id='get_export_uri_failed',
            group_id='cancel_export'
        )

        update_export_name_cancelled = rail.RepliconServiceOperator(
            task_id='update_export_name_cancelled',
            endpoint='/services/TimeDataExportService1.svc/UpdateTimeDataExportName',
            data={
                "target": {
                    "uri": "{{ result('get_export_uri_failed') }}"
                },
                "name": "{{ result('logging_details').time_export_filename_cancelled }}"
            }
        )

        # Send failures for routine (non-alerting) emails must not fail the DAG run. The two
        # failure-alert emails are deliberately excluded: if the alert about a posting/validation
        # failure itself fails to send, that's a silent double-failure (business failure AND nobody
        # notified) and should fail the run rather than disappear behind a green DAG.
        non_critical_task_ids = ['send_empty_export_email', 'send_success_email']
        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ (get_failed_upstream_task_ids() | reject('in', %s) | list | length) > 0 }}" % non_critical_task_ids,
            yes_task='fail_time_export',
            no_task='time_export_finish'
        )

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}'
        )

        time_export_finish = rail.EmptyOperator(
            task_id='time_export_finish'
        )

        # pylint: disable=pointless-statement,expression-not-assigned
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> should_fail_dag
        can_run_batch_task >> rail.Label('No') >> logging_details
        logging_details >> get_time_download_script >> time_export_batch_start

        time_export_batch_end >> create_timeexport_collection >> check_extraction_error
        check_extraction_error >> rail.Label('No') >> finish_oracle_export_batch >> has_data

        has_data >> rail.Label('Yes') >> can_post_to_oracle
        can_post_to_oracle >> rail.Label('Yes') >> filter_eligible_records >> has_eligible_records
        has_eligible_records >> rail.Label('Yes') >> compute_batch_name >> get_user_details
        get_user_details >> build_user_uri_map >> get_primary_function_roles
        get_primary_function_roles >> build_login_to_function_map >> has_resolvable_primary_functions
        has_resolvable_primary_functions >> rail.Label('Yes') >> build_generic_resource_query_task
        has_resolvable_primary_functions >> rail.Label('No') >> log_no_resolvable_functions >> should_fail_dag
        build_generic_resource_query_task >> get_generic_resource_persons
        get_generic_resource_persons >> build_person_number_map >> build_oracle_rows >> has_rows_to_post
        has_rows_to_post >> rail.Label('No') >> log_no_rows_to_post >> should_fail_dag
        has_eligible_records >> rail.Label('No') >> log_no_eligible_records >> should_fail_dag
        can_post_to_oracle >> rail.Label('No') >> skip_oracle_posting_killswitch

        skip_oracle_posting_killswitch >> should_fail_dag

        has_rows_to_post >> rail.Label('Yes') >> build_validate_envelope >> submit_validate_soap >> check_validation_failure
        check_validation_failure >> rail.Label('Yes') >> format_validation_failure_message_task >> log_validation_failure
        log_validation_failure >> build_validation_report_rows >> render_validation_report_csv
        render_validation_report_csv >> generate_validation_report_link >> upload_validation_report_csv_sftp
        upload_validation_report_csv_sftp >> send_validation_failure_email >> mark_export_status_cancel_validation_start
        mark_export_status_cancel_validation_end >> should_fail_dag
        check_validation_failure >> rail.Label('No') >> build_bulk_soap_envelope

        build_bulk_soap_envelope >> submit_bulk_soap >> check_soap_fault
        check_soap_fault >> rail.Label('Yes') >> format_soap_fault_message_task >> log_posting_failure
        log_posting_failure >> build_posting_report_rows >> render_posting_report_csv
        render_posting_report_csv >> generate_posting_report_link >> upload_posting_report_csv_sftp
        upload_posting_report_csv_sftp >> send_posting_failure_email >> should_fail_dag
        check_soap_fault >> rail.Label('No') >> build_success_report_rows >> render_success_report_csv
        render_success_report_csv >> generate_success_report_link >> upload_success_report_csv_sftp
        upload_success_report_csv_sftp >> send_success_email

        has_data >> rail.Label('No') >> update_export_name_to_no_data >> send_empty_export_email

        send_success_email >> should_fail_dag
        send_empty_export_email >> should_fail_dag

        check_extraction_error >> rail.Label('Yes') >> mark_timedata_export_error
        mark_timedata_export_error >> get_export_uri_failed >> mark_export_status_cancel_start
        mark_export_status_cancel_end >> update_export_name_cancelled >> should_fail_dag

        should_fail_dag >> rail.Label('Yes') >> fail_time_export
        should_fail_dag >> rail.Label('No') >> time_export_finish

    return dag


rail.for_each_instance(create_dag)
