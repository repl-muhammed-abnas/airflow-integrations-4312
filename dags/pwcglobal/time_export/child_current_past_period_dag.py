import rail
from pwcglobal.time_export.request_payload import get_paris_timenow_in_fmt, get_transaction_date_list
from pwcglobal.time_export.response_filter import retrieve_export_uri
from pwcglobal.time_export.task.time_data_export import time_data_export


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/time_extract/config.py


# pylint:disable = too-many-statements
def create_child_current_past_period_dag(config):
    current_past_dags = []

    for location in config.location_codes:
        with rail.create_airflow_dag(
            dag_id=f'pwc_time_export_child_current_past_period_{location}_{config.instance}',
            description=f'Timeexport for current and past period {location} {config.instance}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.child_current_past_period_dag_max_active_runs,
            max_active_tasks=config.dag_max_active_tasks,
            default_args={
                'sftp_conn_id': config.sftp_conn_id
            }
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            group_id = 'time_data_export'

            (create_export, write_log_with_no_data,
             finish_time_export_task_group) = time_data_export(group_id)

            invalid_ignored_data = rail.QueryCollectionOperator(
                task_id='invalid_ignored_data',
                query="""SELECT * FROM validateddata WHERE ChargeCode IS NULL OR Timesheet_Start_Date IS NULL"""
            )

            valid_extracted_data = rail.QueryCollectionOperator(
                task_id='valid_extracted_data',
                query="""SELECT * FROM validateddata WHERE ChargeCode IS NOT NULL AND Timesheet_Start_Date IS NOT NULL"""
            )

            is_valid_data_present = rail.IfOperator(
                task_id="is_valid_data_present",
                test="{{ result('valid_extracted_data', 'length') > 0 }}",
                yes_task="render_final_extract_data",
                no_task="mark_export_as_completed"
            )

            render_final_extract_data = rail.WriteCSVFileOperator(
                task_id='render_final_extract_data',
                source="{{ result('valid_extracted_data') }}",
                header=[
                    'TransactionDate',
                    'TimeEntryId',
                    'iwfr\\InternalPerson\\PartyId',
                    'iwfr\\PwCLegalEntity\\PartyId',
                    'Timesheet Start Date',
                    'Timesheet End Date',
                    'HoursQuantity',
                    'Comments',
                    'WorkLocation',
                    'WorkCategory',
                    'ResourceRole',
                    'ChargeCode',
                    'WorkItemType'
                ],
                row=[
                    '{{ item.TransactionDate | sn }}',
                    '{{ item.TimeEntryId | sn }}',
                    '{{ item.iwfr_InternalPerson_PartyId | sn }}',
                    '{{ item.iwfr_PwCLegalEntity_Partyid | sn }}',
                    '{{ item.Timesheet_Start_Date | sn }}',
                    '{{ item.Timesheet_End_Date | sn }}',
                    '{{ item.HoursQuantity | sn }}',
                    '{{ item.Comments | sn }}',
                    '{{ item.WorkLocation | sn }}',
                    '{{ item.WorkCategory | sn }}',
                    '{{ item.ResourceRole | sn }}',
                    '{{ item.ChargeCode | sn }}',
                    '{{ item.WorkItemType | sn }}'
                ]
            )

            log_the_records_count = rail.PythonOperator(
                task_id="log_the_records_count",
                #pylint: disable=line-too-long
                python_callable=lambda: f"{get_paris_timenow_in_fmt()} - INFO admin No of records exported = {rail.result('valid_extracted_data', 'length')}"
            )

            def file_upload_failed(context):
                subject = "{{ get_company_key() }} | Time data export ({{ dag_run.conf.export_period }}) automation - \
                    SFTP failure for {{ dag_run.conf.location }} - {{ current_time('%Y-%m-%dT%H:%M:%S') }}"
                log_upload = 'log' in context['task'].task_id
                file_path = (config.secondary_log_filepath if 'secondary' in context['task'].task_id else config.log_filepath) if log_upload else (
                    config.secondary_upload_filepath if 'secondary' in context['task'].task_id else config.upload_filepath)
                email = rail.EmailOperator(
                    task_id='send_time_data_to_sftp_failure_email',
                    to=config.alert_email,
                    subject=subject,
                    html_content="email_sftp_upload_failed.html",
                    params={
                        # pylint: disable=cell-var-from-loop
                        'dag_id': f'pwc_time_export_child_current_past_period_{location}_{config.instance}',
                        'type': 'log file' if log_upload else 'file',
                        'path': file_path
                    },
                    files=[
                        ("{{ result('render_logs_csv') }}" if log_upload else "{{ result('render_final_extract_data') }}")
                    ]
                )
                email.render_template_fields(context)
                email.execute(context)

            upload_export_file_to_sftp = rail.SFTPUploadFileOperator(
                task_id='upload_export_file_to_sftp',
                content="{{ result('render_final_extract_data') }}",
                remote_filepath=config.upload_filepath +
                '/{{ dag_run.conf.export_file_name }}.csv',
                on_failure_callback=file_upload_failed
            )

            if config.secondary_sftp:
                upload_export_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
                    task_id='upload_export_file_to_secondary_sftp',
                    sftp_conn_id=config.secondary_sftp_conn_id,
                    content="{{ result('render_final_extract_data') }}",
                    remote_filepath=config.secondary_upload_filepath +
                    '/{{ dag_run.conf.export_file_name }}.csv',
                    on_failure_callback=file_upload_failed
                )

            mark_export_as_completed = rail.RepliconServiceOperator(
                task_id="mark_export_as_completed",
                endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
                data={
                    "target": {
                        "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                    }
                }
            )

            mark_timedata_export_draft_error = rail.EmptyOperator(
                task_id='mark_timedata_export_draft_error',
                trigger_rule='one_failed'
            )

            get_export_uri_failed = rail.RepliconServiceOperator(
                task_id='get_export_uri_failed',
                endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
                data={
                    "timeDataExportBatchUri": "{{ result('" + group_id + ".create_export') }}"
                },
                data_handler=retrieve_export_uri
            )

            mark_timedata_export_as_draft = rail.RepliconServiceOperator(
                task_id='mark_timedata_export_as_draft',
                endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsDraft",
                data={
                    "target": {
                        "uri": "{{ result('get_export_uri_failed') }}"
                    }
                }
            )

            cancel_timedata_export = rail.RepliconServiceOperator(
                task_id='cancel_timedata_export',
                endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
                data={
                    "target": {
                        "uri": "{{ result('get_export_uri_failed') }}"
                    }
                }
            )

            fail_time_export = rail.FailOperator(
                task_id='fail_time_export',
                message=config.error_template
            )

            is_valid_data_present_to_log = rail.IfOperator(
                task_id="is_valid_data_present_to_log",
                test="{{ result('valid_extracted_data', 'length') > 0 }}",
                yes_task="get_export_file_log_count",
                no_task="process_invalid_records"
            )

            def get_export_log_count(dag_run):
                paris_time_now = get_paris_timenow_in_fmt()
                return {
                    'logs': [
                        {'log': dag_run.conf['process_start']},
                        {'log': rail.result('log_the_records_count')},
                        {'log': f"{paris_time_now} - INFO admin Export File_{dag_run.conf['export_file_name']}.csv created"},
                        {'log': f"{paris_time_now} - Process ended"}
                    ],
                    'processended': paris_time_now,
                    # pylint: disable=cell-var-from-loop
                    'recordcount': rail.result('valid_extracted_data', 'length')
                }

            get_export_file_log_count = rail.PythonOperator(
                task_id="get_export_file_log_count",
                python_callable=get_export_log_count
            )

            render_logs_csv = rail.WriteCSVFileOperator(
                task_id="render_logs_csv",
                source="{{ result('get_export_file_log_count').logs | to_json }}",
                header=None,
                row=[
                    '{{ item.log }}'
                ]
            )

            upload_log_to_sftp = rail.SFTPUploadFileOperator(
                task_id="upload_log_to_sftp",
                content="{{ result('render_logs_csv') }}",
                remote_filepath=config.log_filepath +
                '/Log_{{ dag_run.conf.export_file_name }}.csv',
                on_failure_callback=file_upload_failed
            )

            if config.secondary_sftp:
                upload_log_to_secondary_sftp = rail.SFTPUploadFileOperator(
                    task_id="upload_log_to_secondary_sftp",
                    content="{{ result('render_logs_csv') }}",
                    remote_filepath=config.secondary_log_filepath +
                    '/Log_{{ dag_run.conf.export_file_name }}.csv',
                    on_failure_callback=file_upload_failed
                )

            send_valid_import_complete_email = rail.EmailOperator(
                task_id="send_valid_import_complete_email",
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                #pylint: disable=line-too-long
                subject='{{ get_company_key() }} | Time data export ({{ dag_run.conf.export_period }}) completed for {{ dag_run.conf.location }} - {{ dag_run.conf.start_time }}',
                html_content="email_valid_import_complete.html",
                params={
                    'upload_file_path': config.upload_filepath,
                    'secondary_upload_filepath': config.secondary_upload_filepath if config.secondary_sftp else None,
                    'log_filepath': config.log_filepath,
                    'secondary_log_filepath': config.secondary_log_filepath if config.secondary_sftp else None
                }
            )

            write_valid_import_log = rail.WriteLogOperator(
                task_id="write_valid_import_log",
                severity="Success",
                message="valid import log",
                properties={
                    "location": "{{ dag_run.conf.location }}",
                    "process_started": "{{ dag_run.conf.process_start_time }}",
                    "process_end": "{{ current_time_in_specified_tz('Europe/Paris', '%Y-%m-%dT%H:%M:%S') }}",
                    "rowcount": "{{ result('get_export_file_log_count').recordcount }}",
                    "filename": "{{ dag_run.conf.export_file_name }}.csv",
                    "datapresent": "yes",
                    "status": "success",
                    "extracttype": "{{ dag_run.conf.export_period }}"
                }
            )

            log_to_sumo_valid_import = rail.SendToSumoOperator(
                task_id="log_to_sumo_valid_import",
                data={
                    'jobstarttime': '{{ dag_run.conf.process_start_time }}',
                    'jobendtime': '{{ current_time_in_specified_tz("Europe/Paris", "%Y-%m-%dT%H:%M:%S") }}',
                    'exportperiod': '{{ dag_run.conf.export_period }}',
                    'exportfilename': '{{ dag_run.conf.export_file_name }}.csv',
                    'exportfilepath': config.upload_filepath,
                    'exportsecondaryfilepath': config.secondary_upload_filepath if config.secondary_sftp else None,
                    'territory': '{{ dag_run.conf.location }}',
                    'numberofrecords': "{{ result('get_export_file_log_count').recordcount }}",
                    'logfilename': 'Log_{{ dag_run.conf.export_file_name }}.csv',
                    'logfilepath': config.log_filepath,
                    'secondarylogfilepath': config.secondary_log_filepath if config.secondary_sftp else None
                },
                sumo_conn_id=config.sumo_conn_id
            )

            process_invalid_records = rail.EmptyOperator(
                task_id='process_invalid_records'
            )

            is_invalid_data_present_to_log = rail.IfOperator(
                task_id="is_invalid_data_present_to_log",
                test="{{ result('invalid_ignored_data', 'length') > 0 }}",
                yes_task="send_invalid_record_email",
                no_task="catch_and_log_errors"
            )

            send_invalid_record_email = rail.EmailOperator(
                task_id="send_invalid_record_email",
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                #pylint: disable=line-too-long
                subject='{{ get_company_key() }} | Blank chargecode or timesheet period entries found in Time extract ({{ dag_run.conf.export_period }}) for {{ dag_run.conf.location }} - {{ dag_run.conf.start_time }}',
                html_content="email_invalid_record.html",
                params={
                    'dag_id': f'pwc_time_export_child_current_past_period_{location}_{config.instance}',
                    'parent_dag_id': f'pwc_time_export_child_location_{location}_{config.instance}'
                }
            )

            is_transaction_date_not_present = rail.IfOperator(
                task_id="is_transaction_date_not_present",
                test=lambda: bool(get_transaction_date_list()),
                yes_task="write_invalid_data_log",
                no_task="catch_and_log_errors"
            )

            write_invalid_data_log = rail.WriteLogOperator(
                task_id="write_invalid_data_log",
                severity="Success",
                message="invalid import log",
                properties={
                    "location": "{{ dag_run.conf.location }}",
                    "process_started": "{{ dag_run.conf.process_start_time }}",
                    "process_end": "{{ current_time_in_specified_tz('Europe/Paris', '%Y-%m-%dT%H:%M:%S') }}",
                    "rowcount": 0,
                    "filename": "{{ dag_run.conf.export_file_name }}.csv",
                    "datapresent": "no",
                    "status": "success",
                    "extracttype": "{{ dag_run.conf.export_period }}"
                }
            )

            log_to_sumo_no_data = rail.SendToSumoOperator(
                task_id="log_to_sumo_no_data",
                data={
                    'jobstarttime': '{{ dag_run.conf.process_start_time }}',
                    'jobendtime': '{{ current_time_in_specified_tz("Europe/Paris", "%Y-%m-%dT%H:%M:%S") }}',
                    'exportperiod': '{{ dag_run.conf.export_period }}',
                    'exportfilename': '{{ dag_run.conf.export_file_name }}_Nodownloaddata.csv',
                    'exportfilepath': config.upload_filepath,
                    'exportsecondaryfilepath': config.secondary_upload_filepath if config.secondary_sftp else None,
                    'territory': '{{ dag_run.conf.location }}',
                    'numberofrecords': 0,
                    'logfilename': None,
                    'logfilepath': config.log_filepath,
                    'secondarylogfilepath': config.secondary_log_filepath if config.secondary_sftp else None
                },
                sumo_conn_id=config.sumo_conn_id
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                message=config.error_template,
                properties={
                    "location": "{{ dag_run.conf.location }}",
                    "process_started": "{{ dag_run.conf.process_start_time }}",
                    "process_end": "{{ current_time_in_specified_tz('Europe/Paris', '%Y-%m-%dT%H:%M:%S') }}",
                    "rowcount": "{{ result('" + group_id + ".create_timeexport_collection', 'length') or \
                        result('valid_extracted_data', 'length') or 0 }}",
                    "filename": "{{ dag_run.conf.export_file_name }}.csv",
                    "datapresent": "no",
                    "status": f"error'|'{config.error_template}",
                    "extracttype": "{{ dag_run.conf.export_period }}"
                }
            )

            dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='dagrun_log_to_sumo',
                sumo_conn_id=config.dagrun_log_sumo_conn_id,
                trigger_rule='all_done',
                extra_info={
                    'location': '{{ dag_run.conf.location }}',
                    'usercount': '{{ dag_run.conf.total_user_count }}',
                    'daterange': "{{ dag_run.conf.twb_start_end_date }}",
                    'twbrowcount': "{{ result('" + group_id + ".create_timeexport_collection', 'length') or \
                        result('valid_extracted_data', 'length') or 0 }}",
                    'filename': "{{ dag_run.conf.export_file_name }}.csv"
                }
            )

            create_export

            finish_time_export_task_group >> invalid_ignored_data >> \
                valid_extracted_data >> is_valid_data_present

            write_log_with_no_data >> log_to_sumo_no_data >> catch_and_log_errors

            is_valid_data_present >> rail.Label(
                "Yes") >> render_final_extract_data >> log_the_records_count

            if config.secondary_sftp:
                log_the_records_count >> [
                    upload_export_file_to_sftp, upload_export_file_to_secondary_sftp] >> mark_export_as_completed
            else:
                log_the_records_count >> upload_export_file_to_sftp >> mark_export_as_completed

            is_valid_data_present >> rail.Label(
                "No") >> mark_export_as_completed

            mark_export_as_completed >> rail.Label(
                "On Error") >> mark_timedata_export_draft_error >> get_export_uri_failed >> mark_timedata_export_as_draft >> \
                cancel_timedata_export >> fail_time_export

            mark_export_as_completed >> is_valid_data_present_to_log

            is_valid_data_present_to_log >> rail.Label(
                "Yes") >> get_export_file_log_count >> render_logs_csv

            if config.secondary_sftp:
                render_logs_csv >> [
                    upload_log_to_sftp, upload_log_to_secondary_sftp] >> send_valid_import_complete_email
            else:
                render_logs_csv >> upload_log_to_sftp >> send_valid_import_complete_email

            send_valid_import_complete_email >> write_valid_import_log >> log_to_sumo_valid_import >> \
                process_invalid_records

            is_valid_data_present_to_log >> rail.Label(
                "No") >> process_invalid_records

            process_invalid_records >> is_invalid_data_present_to_log

            is_invalid_data_present_to_log >> rail.Label(
                "Yes") >> send_invalid_record_email >> is_transaction_date_not_present

            is_transaction_date_not_present >> rail.Label(
                "Yes") >> write_invalid_data_log >> catch_and_log_errors

            is_transaction_date_not_present >> rail.Label(
                "No") >> catch_and_log_errors

            is_invalid_data_present_to_log >> rail.Label(
                "No") >> catch_and_log_errors

            catch_and_log_errors >> dagrun_log_to_sumo

        current_past_dags.append(dag)

    return current_past_dags


rail.for_each_instance(create_child_current_past_period_dag)
