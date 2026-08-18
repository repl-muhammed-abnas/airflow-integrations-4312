from datetime import timedelta
from pendulum import datetime
import pendulum
from capgemini.time_export_mexico_v3_file_based.utils import custom_methods
from capgemini.time_export_mexico_v3_file_based.utils import request_payload
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.time_export_child_dag_id,
        description=f'Capgemini Time export Mexico Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        },
    ) as dag:

        locations = config.export_locations

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_start_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_start_time',
            end_task='export_period',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config]
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=request_payload.get_create_download_batch
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('create_download_batch') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_export') }}",
            delimiter=';'
        )

        create_timeexport_collection = rail.CreateCollectionOperator(
            task_id='create_timeexport_collection',
            name='datatoexport',
            source='{{ result("load_export") }}'
        )

        export_period = rail.PythonOperator(
            task_id='export_period',
            python_callable=custom_methods.get_export_period
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("create_timeexport_collection", "length") > 0 }}',
            yes_task='can_send_downstream',
            no_task='update_export_name_to_no_data'
        )

        can_send_downstream = rail.IfOperator(
            task_id='can_send_downstream',
            test=lambda: Variable.get(config.can_send_time_export_downstream).lower() == "true",
            yes_task='get_export_creation_datetime',
            no_task='time_export_finish'
        )

        update_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id="update_export_name_to_no_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.time_export_uri }}"
                },
                "name": "{{ dag_run.conf.time_export_name }}"
            }
        )

        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon time data extract for ' + locations + ' - No records to export - {{ result("process_start_time") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'location': locations
            }
        )

        log_to_sumo_time_export_no_data = rail.SendToSumoOperator(
            task_id="log_to_sumo_time_export_no_data",
            data={
                'jobstarttime': '{{ result("process_start_time") }}',
                'jobendtime': '{{ current_time_in_specified_tz("UTC", "%Y-%m-%dT%H:%M:%S") }}',
                'exportperiod': '{{ result("export_period").export_start_date }} - {{ result("export_period").export_end_date }}',
                'exportfilename': '{{ dag_run.conf.time_export_name }}',
                'exportfilepath': '',
                'country': locations,
                'numberofrecords': '0',
                'totalhours': '0.0'
            },
            sumo_conn_id=config.sumo_conn_id
        )

        get_export_creation_datetime = rail.RepliconServiceOperator(
            task_id='get_export_creation_datetime',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.time_export_uri }}",
                    "name": null
                }
            },
            data_handler=custom_methods.get_export_datetime
        )

        write_time_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_time_data_to_csv',
            source='{{ result("create_timeexport_collection") }}',
            header=['Reference', 'Transaction_source', 'Batch_name', 'Employee_number', 'Expenditure_item_date',
                'Project_number', 'Task_number', 'Expenditure_type', 'Non Labor resource',
                'Non Labor resource_org_name', 'Organization_name', 'Quantity', 'Expenditure_comment',
                'DFF : Start_date', 'DFF: End_date', 'Quantity in days', 'External application unit of measure for time entry',
                'Attribute3', 'Attribute4', 'Attribute5', 'Attribute6', 'Attribute7', 'Attribute8', 'Attribute9',
                'Nb_hours_sup', 'Raw cost', 'Raw cost rate', 'Billable Flag', 'Export Creation Datetime', 'Row Number'],
            row=lambda item, **context: custom_methods.get_time_data_csv_rows(item, context['index']),
            delimiter=';',
            thread_pool_size=config.thread_pool_size_write_csv,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv)
        )

        upload_time_export_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_time_export_to_s3',
            source='{{ result("write_time_data_to_csv") }}',
            key_name=config.s3_upload_filepath + '/{{ dag_run.conf.time_export_name }}.csv',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_time_export_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_time_data_to_csv') }}"
        )

        upload_time_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_time_export_to_sftp',
            content='{{ result("encrypt_time_export_data_csv") }}',
            remote_filepath=config.input_filepath + '/{{ dag_run.conf.time_export_name }}.csv.pgp'
        )

        query_sum_of_entry_hours = rail.QueryCollectionOperator(
            task_id='query_sum_of_entry_hours',
            query="""SELECT ROUND(SUM(datatoexport.Quantity), 2) AS total_work_hours FROM datatoexport"""
        )

        get_total_hours = rail.PythonOperator(
            task_id='get_total_hours',
            python_callable=lambda: rail.load_all_records(rail.result("query_sum_of_entry_hours"))[0]["total_work_hours"]
        )

        write_timedata_logfile_csv = rail.WriteCSVFileOperator(
            task_id='write_timedata_logfile_csv',
            source=lambda dag_run: custom_methods.get_time_data_log(dag_run, config.input_filepath, locations,
                rail.result("get_total_hours"), rail.result("create_timeexport_collection", "length")),
            header=config.logfile_columns,
            row=custom_methods.get_log_data_rows,
            delimiter=';',
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv)
        )

        upload_timedata_log_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_timedata_log_file_to_sftp',
            content='{{ result("write_timedata_logfile_csv") }}',
            remote_filepath=config.log_filepath + '/log_{{ dag_run.conf.time_export_name }}.csv'
        )

        set_export_filename = rail.SetVariableOperator(
            task_id='set_export_filename',
            name='filename',
            value="{{ dag_run.conf.time_export_name }}.csv.pgp"
        )

        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon time data extract completed for ' + locations + ' - {{ result("process_start_time") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': locations,
                'log_filepath': config.log_filepath,
            }
        )

        log_to_sumo_time_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_time_export",
            data={
                'jobstarttime': '{{ result("process_start_time") }}',
                'jobendtime': '{{ current_time_in_specified_tz("UTC", "%Y-%m-%dT%H:%M:%S") }}',
                'exportperiod': '{{ result("export_period").export_start_date }} - {{ result("export_period").export_end_date }}',
                'exportfilename': '{{ dag_run.conf.time_export_name }}.csv.pgp',
                'exportfilepath': config.input_filepath,
                'country': locations,
                'numberofrecords': "{{ result('create_timeexport_collection', 'length') }}",
                'totalhours': "{{ result('get_total_hours') }}"
            },
            sumo_conn_id=config.sumo_conn_id
        )

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}'
        )

        get_filename = rail.GetVariableOperator(
            task_id='get_filename',
            trigger_rule='all_done',
            name='filename'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            extra_info=lambda dag_run: {
                'locations': locations,
                'daterange': rail.result("export_period")["export_start_date"] + ' - ' + rail.result("export_period")["export_end_date"],
                'twbrowcount': rail.result('create_timeexport_collection', 'length')
                    if rail.result('create_timeexport_collection') and rail.result('create_timeexport_collection', 'length') > 0 else 0,
                'filename': dag_run.conf["time_export_name"],
                'is_exported': "Yes" if Variable.get(config.can_send_time_export_downstream).lower() == "true" else "No"
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_time_export',
            no_task='time_export_finish'
        )

        time_export_finish = rail.EmptyOperator(
            task_id='time_export_finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> export_period
        can_run_batch_task >> rail.Label('No') >> process_start_time

        process_start_time >> logging_details >> create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url
        get_download_url >> download_export >> load_export 

        load_export >> create_timeexport_collection >> export_period >> has_data
        has_data >> rail.Label("Yes") >> can_send_downstream
        can_send_downstream >> rail.Label("Yes") >> get_export_creation_datetime >> write_time_data_to_csv >> upload_time_export_to_s3 \
            >> encrypt_time_export_data_csv >> upload_time_export_to_sftp >> query_sum_of_entry_hours \
                >> get_total_hours >> write_timedata_logfile_csv >> upload_timedata_log_file_to_sftp >> set_export_filename \
                    >> send_valid_export_complete_email >> log_to_sumo_time_export >> get_filename
        can_send_downstream >> rail.Label("No") >> time_export_finish
        has_data >> rail.Label("No") >> update_export_name_to_no_data \
            >> send_empty_export_email >> log_to_sumo_time_export_no_data >> get_filename
        get_filename >> dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_time_export
        should_fail_dag >> rail.Label("No") >> time_export_finish

    return dag

rail.for_each_instance(create_dag)
