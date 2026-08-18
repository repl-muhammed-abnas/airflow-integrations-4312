from datetime import datetime as timedelta
import rail
from eisner_amper.time_and_timeoff_export_to_workday.utils import request_payload


null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"eisner_amper_time_export_child_{config.instance}",
        description=f"Eisner Amper Time Export Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_timedata_download_batch = rail.RepliconServiceOperator(
            task_id="create_timedata_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data=request_payload.get_timedata_download_batch_data
        )

        execute_timedata_batch, wait_fortimedata_batch = rail.batch_execution(
            'execute_payrun_batch', create_timedata_download_batch.task_id)

        get_timedata_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_timedata_download_batch_result",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('create_timedata_download_batch') }}"}
        )

        download_timedata_file = rail.HTTPDownloadFileOperator(
            task_id='download_timedata_file',
            url="{{ result('get_timedata_download_batch_result').downloadUrl }}",
        )
        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_timedata_file') }}"
        )

        create_final_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_time_data_collection',
            name='finaltimedata',
            source="{{ result('load_export') }}",
            columns={
                'Employee ID': 'employeeid',
                'Entry Date': 'entrydate',
                'Hours': 'hours',
                'Project Profile': 'projectprofile',
                'Project Type': 'projecttype',
                'Employee Type Name': 'employeetypename',
                'Company Code Code': 'companycodecode',
                'Cost Center Code': 'costcentercode',
            }
        )

        query_finaltimedata_records = rail.QueryCollectionOperator(
            task_id='query_finaltimedata_records',
            query="SELECT * FROM finaltimedata WHERE" +
            " (projecttype = 'NA' OR projecttype = '10' OR projecttype = '02') AND (employeetypename = 'Hourly – Exempt' " +
            "OR employeetypename = 'Hourly – Exempt' OR employeetypename = 'Hourly – Non-Exempt' OR" +
            " employeetypename = 'Standard – Non-Exempt') AND (costcentercode != 'US01102100' AND costcentercode != 'US01201100'" +
            " AND costcentercode != 'US01202100' )"
        )

        has_distinct_data = rail.IfOperator(
            task_id='has_distinct_data',
            test="{{ result('query_finaltimedata_records', 'length') > 0 }}",
            yes_task='compose_timedata',
            no_task='send_no_data_mail'
        )

        compose_timedata = rail.WriteCSVFileOperator(
            task_id='compose_timedata',
            header=["Header_Line", "Line_Key", "Employee_ID", "Date",
                    "Time_Entry_Code", "Hours"],
            source="{{ result('query_finaltimedata_records') }}",
            row=lambda item, **context: request_payload.get_time_data_csv_rows(
                item, context['index'])
        )

        create_header_line_collection = rail.CreateCollectionOperator(
            task_id='create_header_line_collection',
            name='finaldistincttimedata',
            source="{{ result('compose_timedata') }}"
        )

        query_distinct_header_records = rail.QueryCollectionOperator(
            task_id='query_distinct_header_records',
            query="SELECT DISTINCT Header_Line FROM finaldistincttimedata "
        )

        write_header_xml_file = rail.RenderTemplateOperator(
            task_id='write_header_xml_file',
            target='artifact',
            template_file='output/header_template.xml',
            dataset="{{ result('query_distinct_header_records') }}",
        )

        get_data = rail.PythonOperator(
            task_id='get_data',
            python_callable=request_payload.get_email_file_data,
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='output/row_template.txt',
            dataset="{{ result('compose_timedata') }}"
        )

        upload_to_client_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_client_sftp',
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.client_time_export_path +
            "{{ dag_run.conf['Twbname']}}" + '.xml',
            sftp_conn_id=config.sftp_conn_id
        )

        upload_to_internal_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_internal_sftp',
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.internal_time_export_path +
            "{{ dag_run.conf['Twbname']}}" + '.xml',
            sftp_conn_id=config.sftp_conn_internal_id
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon time extract for Workday- Completed Successfully ' + \
            (timedelta.now()).strftime("%Y%m%d%M%S"),
            html_content="template/completion.html",
            params={
                'filepath': config.client_time_export_path,
                'Created_time': (timedelta.now()).strftime("%Y%m%d%M%S")
            }
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon time extract for Workday- No Data to Export  ' + \
            (timedelta.now()).strftime("%Y%m%d%M%S"),
            html_content="template/no_date.html",
            params={
                'Created_time': (timedelta.now()).strftime("%Y%m%d%M%S")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        create_timedata_download_batch >> execute_timedata_batch >> wait_fortimedata_batch >> get_timedata_download_batch_result\
            >> download_timedata_file >> load_export >> create_final_time_data_collection >> query_finaltimedata_records\
            >> has_distinct_data >> rail.Label("Yes") >> compose_timedata >> create_header_line_collection\
            >> query_distinct_header_records >> write_header_xml_file >> get_data >> write_xml_file\
            >> upload_to_client_sftp >> upload_to_internal_sftp >> send_completion_mail\
            >> log_to_sumo >> can_fail_dag >> fail_dagrun

        has_distinct_data >> rail.Label("No") >> send_no_data_mail

    return dag


rail.for_each_instance(create_child_dag)
