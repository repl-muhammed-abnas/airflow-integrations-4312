
from datetime import timedelta
import rail
from horizonmedia.time_off_sync_v1.utils import formatted_data
from horizonmedia.time_off_sync_v1.utils import request_payload
null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long, unnecessary-lambda
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'horizonmediatimeoff_importmaster{dag_id_postfix}_v1',
        description=f'HorizonMedia - Timeoff_import - Master{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'client_sftp_conn_id': config.client_sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_file_path,
            sftp_conn_id=config.client_sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.client_sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}",
        )
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_processed_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')
        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_file') }}",
        )

        compose_csv_with_md5 = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_md5',
            source="{{ result('parse_csv') }}",
            header=['EmployeeID',
                    'Timeofftype',
                    'StartDate',
                    'EndDate',
                    'Hrs',
                    'Action',
                    'UniqueID',
                    'md5'],
            row=lambda item: formatted_data.create_UID(item)
        )

        create_collection_from_csv_inputdata = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv_inputdata',
            source="{{ result('compose_csv_with_md5') }}",
            name="inputdata",
            # todo update this map from actual csv header for key name
            columns={
                'EmployeeID': 'EmployeeID',
                'Timeofftype': 'Timeofftype',
                'StartDate': 'StartDate',
                'EndDate': 'EndDate',
                'Hrs': 'Hrs',
                'Action': 'Action',
                'UniqueID': 'UniqueID',
                'md5': 'md5'
            }
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.client_sftp_conn_id,
            remote_filepath=f"{config.reference_file_path}/{config.reference_file_name}",
        )
        load_reference_file = rail.LoadCSVFileOperator(
            task_id="load_reference_file",
            document="{{ result('download_reference_file') }}",
        )
        write_reference_file = rail.WriteCSVFileOperator(
            task_id="write_reference_file",
            source="{{ result('load_reference_file') }}",
            header=['EmployeeID',
                    'Timeofftype',
                    'StartDate',
                    'EndDate',
                    'Hrs',
                    'Action',
                    'UniqueID',
                    'md5'],
            row=["{{ item['Employee ID'] }}",
                 "{{ item['Time off type'] }}",
                 "{{ item['Start Date'] }}",
                 "{{ item['End Date'] }}",
                 "{{ item['Hrs'] }}",
                 "{{ item['Action'] }}",
                 "{{ item['Unique ID'] }}",
                 "{{ item['md5'] }}"]
        )

        create_collection_from_referencedata = rail.CreateCollectionOperator(
            task_id='create_collection_from_referencedata',
            source="{{ result('write_reference_file') }}",
            name="referencedata",
            # todo update this map from actual csv header for key name
            columns={
                'EmployeeID': 'EmployeeID',
                'Timeofftype': 'Timeofftype',
                'StartDate': 'StartDate',
                'EndDate': 'EndDate',
                'Hrs': 'Hrs',
                'Action': 'Action',
                'UniqueID': 'UniqueID',
                'md5': 'md5'
            }
        )

        query_list_delta_records = rail.QueryCollectionOperator(
            task_id='query_list_delta_records',
            query="""SELECT * FROM  inputdata WHERE inputdata.md5 NOT IN (SELECT DISTINCT  referencedata.md5 FROM  referencedata)""",
        )

        get_query_data = rail.PythonOperator(
            task_id='get_query_data',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_list_delta_records"))
        )

        if_query_list_delta_records_rows_greater_than_0 = rail.IfOperator(
            task_id='if_query_list_delta_records_rows_greater_than_0',
            test='''{{ result('get_query_data') | length > 0 }}''',
            yes_task="get_report_details",
            no_task="send_mail_nodelta",
        )

        send_mail_nodelta = rail.EmailOperator(
            task_id='send_mail_nodelta',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Time off import Completed file processing is skipped -  {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_for_nodata_format.html"
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )
        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report_group',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        parse_report_csv = rail.LoadCSVFileOperator(
            task_id='parse_report_csv',
            headers=["loginname", "employeeid", "useruri", "User Start Date",
                     "User End Date", "User First Name", "User Email"],
            document="{{ result('run_report_group.get_report_result').reportGenerationResults[0].payload }}",
        )
        report_data_to_dict = rail.PythonOperator(
            task_id='report_data_to_dict',
            python_callable=request_payload.get_user_detail_dict
        )
        get_all_timeofftypes_list = rail.RepliconServiceOperator(
            task_id='get_all_timeofftypes_list',
            endpoint="/services/TimeOffTypeListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris": [
                        "urn:replicon:time-off-type-list-column:name",
                        "urn:replicon:time-off-type-list-column:description",
                        "urn:replicon:time-off-type-list-column:enabled"
                    ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=lambda response: formatted_data.create_timeofftypes_list(
                response.json()['d']['rows'])
        )

        get_all_extension_fields = rail.RepliconServiceOperator(
            task_id='get_all_extension_fields',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:time-off"
            }
        )

        process_entry_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_entry_child',
            items=lambda: rail.result('get_query_data'),
            trigger_dag_id=f'horizonmedia_timeoff_importchild{dag_id_postfix}_v1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=request_payload.process_timeoff_user_conf
        )
        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            dag_runs='{{ result("process_entry_child") }}',
            execution_timeout=timedelta(days=14),
        )
        gather_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_data',
            dag_runs="{{ result('process_entry_child') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )
        get_merged_logs = rail.PythonOperator(
            task_id='get_merged_logs',
            python_callable=request_payload.get_errror_logs
        )
        create_log_csv = rail.WriteCSVFileOperator(
            task_id='create_log_csv',
            source=request_payload.get_merged_log_entries,
            header=['Employee ID',
                    'Time Off Type',
                    'Start Date',
                    'Action',
                    'Unique ID',
                    'Status',
                    'Job ID',
                    'Details'],
            row=lambda item: {
                "column_0": item['employeeid'],
                "column_1": item['timeofftype'],
                "column_2": item['startdate'],
                "column_3": item['action'],
                "column_4": item['uniqueid'],
                "column_5": item['status'],
                "column_6": item['childjobid'],
                "column_7": item['details']
            }.values(),
        )

        write_updated_reference_file = rail.WriteCSVFileOperator(
            task_id="write_updated_reference_file",
            source="{{ result('create_collection_from_csv_inputdata') }}",
            header=['Employee ID',
                    'Time off type',
                    'Start Date',
                    'End Date',
                    'Hrs',
                    'Action',
                    'Unique ID',
                    'md5'],
            row=lambda item: [item['EmployeeID'],
                              item['Timeofftype'],
                              item['StartDate'],
                              item['EndDate'],
                              item['Hrs'],
                              item['Action'],
                              item['UniqueID'],
                              item['md5']]
        )

        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            content="{{ result('create_log_csv') }}",
            sftp_conn_id=config.client_sftp_conn_id,
            remote_filepath=config.client_logs_file_path +
            "/Log_{{ dag_run_ecid() }}_Time Off Sync.csv",
        )

        gather_child_error_data = rail.PythonOperator(
            task_id='gather_child_error_data',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_merged_logs'), 'status', 'Failed', 'status'),
        )

        if_any_failure = rail.IfOperator(
            task_id='if_any_failure',
            test="{{ result('gather_child_error_data') == 'Failed' }}",
            yes_task="send_mail_for_failure_child",
            no_task="send_mail_for_success",
        )

        send_mail_for_failure_child = rail.EmailOperator(
            task_id='send_mail_for_failure_child',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email+','+config.alert_email,
            subject='{{ get_company_key() }} | Timeoff import Completed with failures - {{ current_time_in_specified_tz() }} ',
            html_content="templates/emails/email_for_failiure_format.html"
        )

        send_mail_for_success = rail.EmailOperator(
            task_id='send_mail_for_success',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Timeoff import Completed Successfully - {{ current_time_in_specified_tz() }} ',
            html_content="templates/emails/email_for_success_format.html"
        )

        archive_referencefile = rail.SFTPMoveFileOperator(
            task_id='archive_referencefile',
            # trigger_rule='none_skipped',
            sftp_conn_id=config.client_sftp_conn_id,
            existing_filename=f"{config.reference_file_path}/{config.reference_file_name}",
            new_filename=config.reference_archive_file_path +
            "/{{ dag_run_ecid() | replace(':', '-') }}_" +
                              config.reference_file_name
        )

        upload_reference = rail.SFTPUploadFileOperator(
            task_id='upload_reference',
            sftp_conn_id=config.client_sftp_conn_id,
            content="{{result('write_updated_reference_file')}}",
            remote_filepath=f"{config.reference_file_path}/{config.reference_file_name}",
        )

        archive_processed_file = rail.SFTPMoveFileOperator(
            task_id='archive_processed_file',
            # trigger_rule='none_skipped',
            sftp_conn_id=config.client_sftp_conn_id,
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_file_path +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}",
            # content="",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject='''{{ get_company_key() }}| Timeoff import - Failed to process file''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br/> <br/>Hello Team, <br/> <br/> This is to bring immediate attention to the fact that the Timeoff Import process has encountered a failure, specifically due to issues with processing the input file.</p>
                    <ul> 
                    <li>Input File path: {{ result('new_file_sensor')}}</li>
                    <li>Input file name: {{ result('new_file_sensor') | file_name }}</li>
                    </ul><p> <br/>For any queries, please contact our support team at https://support.deltek.com <br/> <br/>Regards, <br/>Deltek Inc. </p>''',
            params=None,
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )
        new_file_sensor >> download_file >> parse_csv >> compose_csv_with_md5 >> create_collection_from_csv_inputdata >> download_reference_file >> load_reference_file >> write_reference_file >> create_collection_from_referencedata >> query_list_delta_records >> get_query_data >> if_query_list_delta_records_rows_greater_than_0
        download_file >> rail.Label("Always") >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_processed_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        if_query_list_delta_records_rows_greater_than_0 >> rail.Label(
            'Yes') >> get_report_details >> run_report_group_entry >> run_report_group_exit >> parse_report_csv >> report_data_to_dict >> get_all_timeofftypes_list >> get_all_extension_fields >> process_entry_child >> wait_for_process_child >> gather_child_data >> get_merged_logs >> create_log_csv
        create_log_csv >> write_updated_reference_file >> upload_to_sftp >> gather_child_error_data >> if_any_failure
        if_any_failure >> rail.Label(
            'No') >> send_mail_for_success >> archive_referencefile
        if_any_failure >> rail.Label(
            'Yes') >> send_mail_for_failure_child >> archive_referencefile
        archive_referencefile >> upload_reference >> finish
        if_query_list_delta_records_rows_greater_than_0 >> rail.Label(
            'No') >> send_mail_nodelta >> finish
        finish >> send_task_failure_email >> final_status

    return dag


rail.for_each_instance(create_dag)
