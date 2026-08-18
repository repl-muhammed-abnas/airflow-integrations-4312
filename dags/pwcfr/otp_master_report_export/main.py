from pendulum import datetime, now
from pwcfr.otp_master_report_export.tasks.report_export import create_report_collection_for_export
import rail

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcfr/otp_master_report_export/config.py"

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id = f"pwcfr_otp_report_export_master_{config.instance}",
        description = f"OTPs(project) report 1_master_otp_report1-3 aggregate {config.instance}",
        company_key= config.company_key,
        max_active_runs = config.max_active_runs,
        schedule_interval= config.schedule_interval,
        start_date= datetime(2023, 6,1,tz=config.cest_time_zone),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            "sftp_conn_id":config.sftp_conn_id
        }
    ) as dag:
        #timezone for filename acc workato recipe
        get_otp_export_file_name = rail.PythonOperator(
            task_id = 'get_otp_export_file_name',
            python_callable=lambda: config.otp_file_name_prefix +(now(tz='PST8PDT')).strftime("%d%m%Y") +'.csv',
        )

        export_collection_report1 = create_report_collection_for_export(config.otp_master_report_1,
                                                                        task_suffix="report1")
        export_collection_report2 = create_report_collection_for_export(config.otp_master_report_2,
                                                                        task_suffix="report2")
        export_collection_report3 = create_report_collection_for_export(config.otp_master_report_3,
                                                                        task_suffix="report3")

        query_combine_otp_reports = rail.QueryCollectionOperator(
            task_id = "query_combine_otp_reports",
            query="""SELECT * FROM otp_master_report1 UNION
                    SELECT * FROM otp_master_report2 UNION
                    SELECT * FROM otp_master_report3"""
        )

        has_otp_export_data = rail.IfOperator(
            task_id = "has_export_data",
            test= '{{result("query_combine_otp_reports","length") > 0}}',
            yes_task="write_otp_export_to_csv",
            no_task="send_no_export_data_found_mail"
        )

        write_otp_export_to_csv = rail.WriteCSVFileOperator(
            task_id="write_otp_export_to_csv",
            source=lambda:rail.result("query_combine_otp_reports"),
            header=["OTP Name", "OTP Code", "OTP Status", "Time & Expense Entry Type","Prject Profile Center"],
            row=lambda item:[
                item['otpname'] if item['otpname'] else None,
                item['otpcode'] if item['otpcode'] else None,
                item['otpstatus'] if item['otpstatus'] else None,
                item['timeandexpenseentrytype'] if item['timeandexpenseentrytype'] else None,
                item['projectprofilecenter'] if item['projectprofilecenter'] else None
            ]
        )

        upload_otp_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_otp_export_to_sftp",
            sftp_conn_id=config.sftp_conn_id,
            content="{{result('write_otp_export_to_csv')}}",
            remote_filepath = config.sftp_export_filepath + "{{result('get_otp_export_file_name')}}"

        )

        send_no_export_data_found_mail = rail.EmailOperator(
            task_id="send_no_export_data_found_mail",
            to=config.tenant_email,
            subject="{{get_company_key()}}| OTP monitoring report export - No data found in the report",
            html_content="<p>No data found in the report</p>"
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

        get_otp_export_file_name >> export_collection_report1 >> query_combine_otp_reports
        get_otp_export_file_name >> export_collection_report2 >> query_combine_otp_reports
        get_otp_export_file_name >> export_collection_report3 >> query_combine_otp_reports >> \
        has_otp_export_data >> rail.Label("Yes") >> write_otp_export_to_csv >> upload_otp_export_to_sftp >> \
        log_to_sumo >> can_fail_dag >> fail_dagrun
        has_otp_export_data >> rail.Label("No") >> send_no_export_data_found_mail >> \
        log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag

rail.for_each_instance(create_main_dag)
