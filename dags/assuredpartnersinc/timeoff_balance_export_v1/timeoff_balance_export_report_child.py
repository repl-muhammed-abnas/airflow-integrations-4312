import rail
from assuredpartnersinc.timeoff_balance_export_v1.utils import custom_methods
from assuredpartnersinc.timeoff_balance_export_v1.tasks.timeoff_data import get_timeoff_data

# pylint: disable=too-many-statements

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"assuredpartnersinc_timeoff_balance_export_report_child_dag_{config.instance}_v1",
        description=f"AssuredpartnersInc Timeoff Balance Export Report {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        unique_users_timeoff_data = rail.QueryCollectionOperator(
            task_id='unique_users_timeoff_data',
            query='''SELECT DISTINCT employeeid, companycode
                        FROM processed_timeoff_data
                        WHERE employeeid != "" AND headercode != ""
                ''',
            name='unique_users_timeoff_data'
        )

        query_pto_1_code_data, process_user_pto_data_finish = get_timeoff_data(
            "pto_1", "user_pto", "user_pto_data_columns")

        query_holiday_code_data, process_user_pto_holiday_data_finish = get_timeoff_data(
            "holiday", "user_pto_holiday", "user_pto_holiday_data_columns")

        query_sick_code_data, process_user_pto_holiday_sick_data_finish = get_timeoff_data(
            "sick", "user_pto_holiday_sick", "user_pto_holiday_sick_data_columns")

        query_volunteer_code_data, process_user_pto_hday_sick_vol_data_finish = get_timeoff_data(
            "volunteer", "user_pto_hday_sick_vol", "user_pto_hday_sick_vol_data_columns")

        write_timeoff_data_csv = rail.WriteCSVFileOperator(
            task_id='write_timeoff_data_csv',
            source='{{ result("user_pto_hday_sick_vol_data") }}',
            header=["Emp No", "PTO-1 Code", "Period Begin Date", "Period End Date", "Allowed Balance",
                    "Taken Balance", "Holiday Code", "Period Begin Date", "Period End Date", "Allowed Balance",
                    "Taken Balance", "Sick Code", "Period Begin Date", "Period End Date", "Allowed Balance",
                    "Taken Balance", "Volunteer  Code", "Period Begin Date", "Period End Date", "Allowed Balance",
                    "Taken Balance", "blank1", "blank2", "blank3", "blank4", "blank5", "blank6", "blank7", "blank8",
                    "blank9", "blank10", "blank11"],
            row=custom_methods.get_users_timeoff_rows,
            lineterminator='\n'
        )

        filename = '/{{dag_run.conf.dag_run_ecid}}_Time off Balance Export to Workday_{{dag_run.conf.jobdateformatted}}.csv'

        def file_upload_failed(context):
            subject = "{{ get_company_key() }} | Time off balance export for Ultipro - Failed to upload - {{ dag_run.conf.dag_start_date }}"
            body = "{{ get_company_key() }} | Time off balance export for Ultipro - Failed to upload - {{ dag_run.conf.dag_start_date }}"
            email = rail.EmailOperator(
                task_id='send_timeoff_data_to_sftp_failure_email',
                to=config.alert_email,
                subject=subject,
                html_content=body,
                files=[
                    ("{{ result('write_timeoff_data_csv') }}")
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_timeoff_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_timeoff_data_to_secondary_sftp',
            content='{{ result("write_timeoff_data_csv") }}',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.sftp_path+filename,
            on_failure_callback=file_upload_failed
        )

        export_complete_email_body = '''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />
            Hello, <br /> <br /> The Time off balance export for Ultipro is completed successfully. 
            Please find the export file - ''' + filename \
            + ''' placed in the below SFTP location.&nbsp;<br /> <br />SFTP Host:&nbsp;fe01.ultipro.com<br /> <br />
            For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'''

        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Time off balance export for Ultipro - Completed - {{ dag_run.conf.dag_start_date }}",
            html_content=export_complete_email_body,
        )

        unique_users_timeoff_data >> query_pto_1_code_data
        process_user_pto_data_finish >> query_holiday_code_data
        process_user_pto_holiday_data_finish >> query_sick_code_data
        process_user_pto_holiday_sick_data_finish >> query_volunteer_code_data
        process_user_pto_hday_sick_vol_data_finish >> write_timeoff_data_csv
        write_timeoff_data_csv >> upload_timeoff_data_to_secondary_sftp \
            >> send_export_complete_email

    return dag


rail.for_each_instance(create_child_dag)
