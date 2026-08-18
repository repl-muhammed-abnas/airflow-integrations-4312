import rail
from pendulum import datetime
from pwcglobal.ipower_time_data_export.utils import request_payload
from pwcglobal.ipower_time_data_export.utils import response_filter
from pwcglobal.ipower_time_data_export.utils import python_callable
from pwcglobal.ipower_time_data_export.utils import custom_methods

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/ipower_time_data_export/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_time_data_export_to_ipower_{config.instance}_{config.location_name}",
        description=f"PWC Global Time Data Export Master to iPower {config.instance} {config.location_name}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    )as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config]
        )

        get_countries_hierarchy_data = rail.RepliconServiceOperator(
            task_id='get_countries_hierarchy_data',
            endpoint='/services/LocationListService1.svc/GetHierarchyData',
            data=request_payload.get_country_hierarchy_payload,
            response_filter=response_filter.filter_location_hierarchy_data
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        create_report_list = rail.PythonOperator(
            task_id='create_report_list',
            python_callable=python_callable.create_report_list,
            op_args=[config]
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_report_generate_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        is_batch_process_failed = rail.IfOperator(
            task_id='is_batch_process_failed',
            test='{{ result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy }}',
            yes_task="fail_batch_process",
            no_task="report_has_data"
        )

        fail_batch_process = rail.FailOperator(
            task_id="fail_batch_process",
            message='{{ result("run_report.get_report_result").reportGenerationResults[0].error }}',
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_csv',
            no_task= 'finish'
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        create_time_data = rail.CreateCollectionOperator(
            task_id='create_time_data',
            source="{{ result('load_csv') }}",
            name='time_data'
        )

        final_data = rail.QueryCollectionOperator(
            task_id='final_data',
            query="SELECT * FROM time_data WHERE Activity_Name != '' AND Activity_Name != 'Null'",
        )

        write_sftp_output_filename = rail.RenderTemplateOperator(
            task_id='write_sftp_output_filename',
            target='result',
            template=config.output_filepath +
            '{{ dag_run_ecid() | replace(":", "-") }}{{ result("get_logging_details").output_filename }}'
        )

        load_final_data_to_csv = rail.WriteCSVFileOperator(
            task_id='load_final_data_to_csv',
            source="{{result('final_data')}}",
            header=["StaffCode", "ClientCode", "JobCode", "AsgOfficeCode",
                    "ProjectCode", "ProjectYear", "Date", "Hours", "Memo"],
            row=request_payload.get_csv_rows,
            lineterminator='\n'
        )

        def file_upload_failed(context):
            subject = "{{ get_company_key() }} | Time Export - Uploading Logs to SFTP failed {{ result('get_logging_details').dag_run_start_time }}"
            body = '''<p>Hi Team,<br /> <br />
                    The time export for {{ get_company_key() }}, hosted on ''' + config.username + ''',
                    created on: {{ result('get_logging_details').dag_run_start_time }} has been completed,
                    however, the log upload to sftp has failed. 
                    Attached is the log file for reference.</p>
                    <ul>
                        <li>DAG ID: '''+dag.dag_id+'''</li>
                        <li>Job ID: {{ dag_run_ecid() }}</li>
                    </ul>
                    <p>Please find the attached logs which was to be sent to intended recipients and debug the issue related to sftp upload.<br /> <br /> 
                    Regards,<br /> 
                    Deltek Inc</p>
                    '''
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.alert_email,
                subject=subject,
                html_content=body
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content='{{ result("load_final_data_to_csv") }}',
            remote_filepath='{{ result("write_sftp_output_filename") }}',
            on_failure_callback=file_upload_failed
        )

        success_email = """<p><strong>This is an automated mail, please don't reply</strong><br /> <br />
                            Hello, <br /> <br />
                            The Time Data Export from Replicon to iPower has been completed successfully. 
                            The export file is placed at
                            {{ result('write_sftp_output_filename') }}.<br /><br />
                            For any queries, please contact our support team at https://support.deltek.com
                            <br /> <br />Regards, <br />Deltek Inc.</p>"""

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }}' +
            ' | '+config.location_name +
            ' - Time Data Export from Replicon to iPower has completed - {{ result("get_logging_details").dag_run_start_time }}',
            html_content=success_email
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_logging_details >> get_countries_hierarchy_data >> get_report_details >> create_report_list >> run_my_report_entry
        run_my_report_exit >> is_batch_process_failed
        is_batch_process_failed >> rail.Label("No") >> report_has_data
        is_batch_process_failed >> rail.Label("Yes") >> fail_batch_process
        report_has_data >> rail.Label("Yes") >> load_csv >> create_time_data >> final_data >> load_final_data_to_csv >> write_sftp_output_filename \
            >> upload_csv_to_sftp >> send_success_email
        report_has_data >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_dag)
