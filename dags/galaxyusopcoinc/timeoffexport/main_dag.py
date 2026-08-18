from datetime import timedelta
from pendulum import datetime
import rail
from galaxyusopcoinc.timeoffexport.utils import response_filter
from galaxyusopcoinc.timeoffexport.utils import request_payload
from galaxyusopcoinc.timeoffexport.task.extract_timeoff import extract_timeoff
from airflow.models import Variable

# config
# https://github.com/replicon/airflow-integrations/blob/main/dags/galaxyusopcoinc/timeoffexport/config.py

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_timeoff_export_master_{config.instance}',
        description=f'Export Timeoff Bookings data to Workday {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        timeoff_export_download_script = rail.RepliconServiceOperator(
            task_id='timeoff_export_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            response_filter=response_filter.get_script_uri
        )

        log_export_name = rail.PythonOperator(
            task_id='log_export_name',
            python_callable=request_payload.log_export_name
        )

        create_timeoffdata_row_counts_batch = rail.RepliconServiceOperator(
            task_id='create_timeoffdata_row_counts_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataItemRowCountsBatch',
            data=request_payload.get_timeoffdata_row_counts_batch_payload(config.pacific_timezone)
        )

        (execute_row_counts_batch, wait_for_row_counts_batch) = rail.batch_execution(
            group_id='execute_row_counts_batch',
            creation_task_id=create_timeoffdata_row_counts_batch.task_id,
            wait_timeout=60*60*5,
        )

        get_timeoffdata_row_counts_results = rail.RepliconServiceOperator(
            task_id='get_timeoffdata_row_counts_results',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataItemRowCountsBatchResults',
            data=request_payload.get_timeoffdata_row_counts_results_payload
        )

        export_has_data = rail.IfOperator(
            task_id='export_has_data',
            test=lambda: rail.result('get_timeoffdata_row_counts_results')[
                'rowCounts'][0] > 0,
            yes_task='process_timeoff_export',
            no_task='send_empty_export_email'
        )

        process_timeoff_export = rail.EmptyOperator(
            task_id='process_timeoff_export'
        )

        extract_timeoff_group_entry, extract_timeoff_group_exit = extract_timeoff(
            'extract_timeoff', lambda: rail.result('timeoff_export_download_script'), lambda: rail.result('log_export_name'),config.pacific_timezone)

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon timeoff export - No records to export - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_empty_export.html"
        )

        get_all_time_off_types_uris = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_uris',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=response_filter.get_filter_timeoff_uris
        )

        get_timeoff_units = rail.RepliconServiceOperator(
            task_id='get_timeoff_units',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=request_payload.get_timeoff_units_payload
        )

        create_export_data_collection = rail.CreateCollectionOperator(
            task_id='create_export_data_collection',
            source="{{ result('load_export') }}",
            columns={
                "EmployeeID": "employeeid",
                "TimeOffEntryID": "timeoffentryid",
                "TimeOffDate": "timeoffdate",
                "TimeOffAmount": "timeoffamount",
                "RefrenceID(TimeOffCode)": "timeoffdescription",
                "StartTime": "starttime",
                "EndTime": "endtime",
                "TimeOffType": "timeofftype",
                "PositionID": "positionid",
                "Comments": "comments",
                "Units": "units"
            },
            name='create_export_data_collection'
        )

        filter_timeoff_type = rail.QueryCollectionOperator(
            task_id='filter_timeoff_type',
            query=config.timeoff_filter_query,
            name='filter_timeoff_type'
        )

        filter_export_data = rail.QueryCollectionOperator(
            task_id='filter_export_data',
            query='''SELECT * from filter_timeoff_type WHERE NULLIF(employeeid, '') IS NOT NULL and NULLIF(timeofftype, '') IS NOT NULL '''
        )

        export_has_valid_data = rail.IfOperator(
            task_id='export_has_valid_data',
            test=lambda: (rail.result('filter_timeoff_type',
                          'length') == rail.result('filter_export_data', 'length')),
            yes_task='create_export_file',
            no_task='create_export_status_draft_batch'
        )

        create_export_status_draft_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_draft_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_batch_payload("draft")
        )

        execute_export_status_draft_batch, wait_for_export_status_draft_batch = rail.batch_execution(
            group_id='execute_time_export_status_draft_batch',
            creation_task_id=create_export_status_draft_batch.task_id
        )

        create_export_status_cancel_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_cancel_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_batch_payload("cancel")
        )

        execute_export_status_cancel_batch, wait_for_export_status_cancel_batch = rail.batch_execution(
            group_id='execute_time_export_status_cancel_batch',
            creation_task_id=create_export_status_cancel_batch.task_id
        )

        send_cancelled_export_email = rail.EmailOperator(
            task_id='send_cancelled_export_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon timeoff export - Invalid records found - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_invalid_records.html"
        )

        create_export_file = rail.WriteCSVFileOperator(
            task_id='create_export_file',
            source=lambda: rail.result('filter_export_data'),
            header=['EmployeeID', 'TimeOffEntryID', 'TimeOffDate', 'TimeOffAmount',
                    'ReferenceID(TimeOffCode)', 'StartTime', 'EndTime', 'TimeOffType', 'PositionID', 'Comments', 'Units'],
            row=request_payload.translate_row,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            delimiter='|',
            thread_pool_size=config.thread_pool_size_write_csv
        )

        upload_file_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_file_to_s3',
            source='{{ result("create_export_file") }}',
            key_name=config.s3_upload_path + "/{{result('log_export_name')}}.csv",
            bucket_name=lambda: Variable.get(
                config.bucket_name, default_var='replicon-airflow-dev-group'),
            aws_conn_id=config.aws_conn_id,
        )

        encrypt_export_file = rail.PGPEncryptionOperator(
            task_id="encrypt_export_file",
            source="{{ result('create_export_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        send_export_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_export_file_to_sftp',
            content="{{ result('encrypt_export_file') }}",
            remote_filepath=config.sftp_upload_path + '/' +
            "{{result('log_export_name')}}" + '.csv',
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon timeoff export - Completed Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_export_success.html",
            params={
                'sftp_upload_path': config.sftp_upload_path
            }
        )

        [timeoff_export_download_script,
                  log_export_name] >> create_timeoffdata_row_counts_batch >> execute_row_counts_batch >> wait_for_row_counts_batch
        wait_for_row_counts_batch >> get_timeoffdata_row_counts_results >> export_has_data >> rail.Label(
            'Yes') >> process_timeoff_export >> extract_timeoff_group_entry
        export_has_data >> rail.Label('No') >> send_empty_export_email
        extract_timeoff_group_exit >> get_all_time_off_types_uris
        get_all_time_off_types_uris >> get_timeoff_units >> create_export_data_collection
        create_export_data_collection >> filter_timeoff_type >> filter_export_data >> export_has_valid_data >> rail.Label(
            'No') >> create_export_status_draft_batch >> execute_export_status_draft_batch >> wait_for_export_status_draft_batch >> \
        create_export_status_cancel_batch >> execute_export_status_cancel_batch >> wait_for_export_status_cancel_batch >> send_cancelled_export_email
        export_has_valid_data >> rail.Label('Yes') >> create_export_file >> upload_file_to_s3 >> encrypt_export_file
        encrypt_export_file >> send_export_file_to_sftp >> send_success_email

    return dag


rail.for_each_instance(create_main_dag)
