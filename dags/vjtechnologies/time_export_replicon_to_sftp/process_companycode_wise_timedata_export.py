import rail
from vjtechnologies.time_export_replicon_to_sftp.utils import python_callable
from vjtechnologies.time_export_replicon_to_sftp.utils import request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vjtechnologies_companycode_wise_timedata_export_child_{config.instance}',
        description=f'vjtechnologies_companycode_wise_timedata_export_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        schedule_interval = None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config_child", extra_config=config)

        get_current_datetime = rail.PythonOperator(
            task_id="get_current_datetime",
            python_callable=python_callable.get_current_date_time
        )

        get_division_details = rail.RepliconServiceOperator(
            task_id="get_division_details",
            endpoint="/services/DivisionService1.svc/GetDivisionDetails",
            data=lambda dag_run: {
                "divisionUri": dag_run.conf['companycode_uri']
            }
        )

        check_for_sftp_filepath = rail.PythonOperator(
            task_id = "check_for_sftp_filepath",
            python_callable=python_callable.check_sftp_input_filepath
        )

        is_input_filepath_present = rail.IfOperator(
            task_id='is_input_filepath_present',
            test='{{ result("check_for_sftp_filepath").input_filepath | is_truthy }}',
            yes_task='get_file_name'
        )

        get_file_name = rail.PythonOperator(
            task_id = "get_file_name",
            python_callable=lambda : rail.result('get_current_datetime') + "_hours_" + rail.result('get_division_details')['code']
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=request_payload.create_timedata_download_batch_payload
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
            delimiter=','
        )

        timedata_list_collection = rail.CreateCollectionOperator(
            task_id="timedata_list_collection",
            source="{{ result('load_export') }}",
            columns={
                "Entry Date": "entrydate",
                "Company Code Code": "companycodecode",
                "Employee ID": "employeeid",
                "Project Name": "projectname",
                "Project Code": "projectcode",
                "Task Name": "taskname",
                "Task Code": "taskcode",
                "Task Description": "taskdescription",
                "Hours": "hours"
            },
            name="timedata"
        )

        is_timedata_present = rail.IfOperator(
            task_id='is_timedata_present',
            test='{{ result("timedata_list_collection", "length") > 0 }}',
            yes_task='query_from_timedata_list'
        )

        query_from_timedata_list = rail.QueryCollectionOperator(
            task_id="query_from_timedata_list",
            query="""SELECT * FROM timedata""",
            name="timedata_result"
        )

        compose_csv_with_header = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_header',
            source="{{ result('query_from_timedata_list') }}",
            header=[
                'Entry Date',
                'Company Code Code',
                'Employee ID',
                'Project Name',
                'Project Code',
                'Task Name',
                'Task Code',
                'Task Description',
                'Hours'
            ],
            row=python_callable.get_row
        )

        create_export_batch = rail.RepliconServiceOperator(
            task_id='create_export_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.create_timedata_export_batch_payload
        )

        execute_export_batch, wait_for_export_batch = rail.batch_execution(
            group_id='execute_export_batch',
            creation_task_id=create_export_batch.task_id
        )

        get_export_batch_result = rail.RepliconServiceOperator(
            task_id='get_export_batch_result',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('create_export_batch') }}"
            },
            data_handler=lambda response: response['timeDataExportUri']
        )

        update_timedata_export_name = rail.RepliconServiceOperator(
            task_id='update_timedata_export_name',
            endpoint='/services/TimeDataExportService1.svc/UpdateTimeDataExportName',
            data={
                "target": {
                    "uri": "{{ result('get_export_batch_result')}}",
                    "name": None
                },
                "name": "{{ result('get_file_name') }}"
                }
        )

        mark_timedata_export_complete = rail.RepliconServiceOperator(
            task_id='mark_timedata_export_complete',
            endpoint='/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete',
            data={
                "target": {
                    "uri": "{{ result('get_export_batch_result')}}",
                    "name": None
                }
            }
        )

        is_export_success = rail.IfOperator(
            task_id='is_export_success',
            test='{{ get_task_state("mark_timedata_export_complete") == "success" }}',
            yes_task='list_sftp_files',
            no_task='cancel_timedata_export',
        )

        cancel_timedata_export = rail.RepliconServiceOperator(
            task_id='cancel_timedata_export',
            endpoint='/services/TimeDataExportService1.svc/CancelTimeDataExport',
            data={
                "target": {
                    "uri": "{{ result('get_export_batch_result')}}",
                    "name": None
                }
            }
        )

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            paths=[config.csv_filepath + "{{ result('check_for_sftp_filepath').input_filepath }}"]
        )

        check_for_filename = rail.PythonOperator(
            task_id = "check_for_filename",
            python_callable=lambda : python_callable.check_filename(config.csv_filepath)
        )

        is_any_filename_present = rail.IfOperator(
            task_id = "is_any_filename_present",
            test='{{ result("check_for_filename") | length > 0}}',
            yes_task='foreach_file_in_directory',
            no_task='upload_csv_to_sftp',
        )

        foreach_file_in_directory = rail.ForEachOperator(
            task_id='foreach_file_in_directory',
            items=lambda : rail.result('check_for_filename'),
            start_task='rename_movethefilefrom_outbound_to_archive',
            end_task='foreach_file_in_directory_end'
        )

        rename_movethefilefrom_outbound_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_movethefilefrom_outbound_to_archive',
            existing_filename="{{ result('foreach_file_in_directory').input_path }}",
            new_filename="{{ result('foreach_file_in_directory').archive_path }}"
        )

        foreach_file_in_directory_end = rail.EmptyOperator(
            task_id='foreach_file_in_directory_end',
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content="{{ result('compose_csv_with_header') }}",
            remote_filepath=config.csv_filepath + "{{ result('check_for_sftp_filepath').input_filepath }}" + "/" + "{{ result('get_file_name') }}" + ".csv"
        )

        is_upload_csv_success = rail.IfOperator(
            task_id='is_upload_csv_success',
            test='{{ get_task_state("upload_csv_to_sftp") == "success" }}',
            yes_task='send_extract_completion_mail',
            no_task='send_extract_error_mail',
        )

        send_extract_completion_mail = rail.EmailOperator(
            task_id='send_extract_completion_mail',
            to=config.to_email,
            subject=f'{config.company_key} | Time extract completed for ' + "{{ dag_run.conf['companycode'] }}" + "-" + "{{ result('get_current_datetime') }}",
            html_content="templates/email/send_extract_completion_email.html",
            params={
                'csv_filepath': config.csv_filepath
            }
        )

        send_extract_error_mail = rail.EmailOperator(
            task_id='send_extract_error_mail',
            to=config.alert_email,
            subject=f'{config.company_key} | Time data export automation - SFTP failure for  ' + \
                "{{ dag_run.conf['companycode'] }}" + "-" + "{{ result('get_current_datetime') }}",
            html_content="templates/email/send_extract_error_email.html",
            files=[
                    ("{{ result('get_file_name') }}.csv", "{{result('compose_csv_with_header')}}")
                ],
            params={
                'username': config.user_name
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "company_code": "{{dag_run.conf.companycode}}",
                "status": "Error",
                "error": '{{ get_error_message() }}'
            }
        )

        log_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        get_current_datetime >> get_division_details >> check_for_sftp_filepath >> is_input_filepath_present >> \
            rail.Label("Yes") >> get_file_name >> create_download_batch >> execute_download_batch >> \
                wait_for_download_batch >> rail.Label("on_success") >> get_download_url >> download_export >> \
                    load_export >> timedata_list_collection >> is_timedata_present

        wait_for_download_batch >> rail.Label("on_error") >> catch_and_log_error >> log_sumo

        is_timedata_present >> rail.Label('Yes') >> query_from_timedata_list >> compose_csv_with_header >> \
            create_export_batch >> execute_export_batch >> \
                wait_for_export_batch >> rail.Label("on_success") >> get_export_batch_result >> \
                update_timedata_export_name >> mark_timedata_export_complete >> is_export_success

        is_export_success >> rail.Label("Yes") >> list_sftp_files >> check_for_filename >> is_any_filename_present

        is_any_filename_present >> rail.Label("No") >> upload_csv_to_sftp >> is_upload_csv_success
        is_any_filename_present >> rail.Label("Yes") >> foreach_file_in_directory >> rename_movethefilefrom_outbound_to_archive >> \
            foreach_file_in_directory_end

        foreach_file_in_directory >> foreach_file_in_directory_end >> upload_csv_to_sftp >> is_upload_csv_success

        is_upload_csv_success >> rail.Label("Yes") >> send_extract_completion_mail >> log_sumo

        is_upload_csv_success >> rail.Label("No") >> send_extract_error_mail >> catch_and_log_error

        is_export_success >> rail.Label("No") >> cancel_timedata_export >> catch_and_log_error

        wait_for_export_batch >> rail.Label("on_error") >> catch_and_log_error

        catch_and_log_error >> log_sumo

        return dag

rail.for_each_instance(create_dag)
