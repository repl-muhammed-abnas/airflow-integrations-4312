import json
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
from pendulum import datetime, from_format, now
from strayeruniversity.time_off_balance_export.custom_methods import get_timeoff_report_params, get_timeoff_data
import rail
null=None

#pylint:disable=too-many-statements

def get_dagrun_schedule(config, dag_run):
    last_execution_date = Variable.get(config.last_execution_date, default_var="21/8/2023")
    last_execution_date = from_format(last_execution_date, "DD/MM/YYYY")
    time_period = (now(tz=config.time_zone) - last_execution_date).days
    today_datetime = now(tz=config.time_zone)
    if dag_run.conf.get("end_date") or (abs(time_period) == 14 and today_datetime.day_of_week == 1 and today_datetime.hour == 17):
       return True
    return False

def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"strayeruniversity_timeoff_balance_export_master_{config.instance}",
        description="strayer university time off balance export master",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 8, 21,17,0, 0, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_dag_runs,
        default_args={
            "sftp_conn_id":config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        if_fourteen_days_since_last_dag_run = rail.IfOperator(
            task_id="if_fourteen_days_since_last_dag_run",
            test=lambda dag_run: get_dagrun_schedule(config, dag_run),
            yes_task="update_last_execution_date",
            no_task="delete_dag_run"
        )
        
        update_last_execution_date = rail.PythonOperator(
            task_id="update_last_execution_date",
            python_callable=lambda:Variable.set(config.last_execution_date,now(tz=config.time_zone).strftime("%d/%m/%Y"))
        )

        delete_dag_run = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dag_run"
        )

        strayeruniversity_log_lookup_table = rail.CreateLogOperator(
            task_id="strayeruniversity_log_lookup_table"
        )
        get_time_off_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_time_off_report_details",
            report_name=config.time_off_report,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id="run_timeoff_report",
            report_params=lambda dag_run, config=config:get_timeoff_report_params(dag_run, config)
        )

        if_report_generation_successful = rail.IfOperator(
            task_id="if_report_generation_successful",
            test='{{result("run_timeoff_report.get_report_result").reportGenerationResults[0].error|is_falsy}}',
            yes_task="if_timeoff_data_not_present",
            no_task="write_report_generation_fail_log"
        )

        write_report_generation_fail_log = rail.WriteLogOperator(
            task_id="write_report_generation_fail_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="Time off balance extract utility process Stopped",
            properties=lambda:json.dumps({
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"***** Time off balance extract utility process Stopped *****",
                "Blank":null
            })
        )

        if_timeoff_data_not_present = rail.IfOperator(
            task_id="if_timeoff_data_not_present",
            test='{{result("run_timeoff_report.get_report_result", "has_data")}}',
            yes_task="write_timeoff_data_found_log",
            no_task="write_no_data_log"
        )

        write_no_data_log = rail.WriteLogOperator(
            task_id="write_no_data_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="No Time Off Balance data found to be exported",
            properties=lambda: json.dumps({
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"No Time Off Balance data found to be exported",
                "Blank":null
            })
        )

        write_timeoff_data_found_log = rail.WriteLogOperator(
            task_id="write_timeoff_data_found_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="Data is obtained from User Time off Balance Details report",
            properties=lambda: json.dumps({
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"Data is obtained from User Time off Balance Details report",
                "Blank":null
            })
        )

        parse_timeoff_csv = rail.LoadCSVFileOperator(
            task_id="parse_timeoff_csv",
            document='{{result("run_timeoff_report.get_report_result").reportGenerationResults[0].payload}}',
        )


        create_user_timeoff_collection = rail.CreateCollectionOperator(
            task_id="create_user_timeoff_collection",
            source=get_timeoff_data,
            name="employeetimeoffdata",
            columns={
                    "Employee ID":"employeeid",
                    "Time Off Type": "timeofftype",
                    "Balance (As of End Date)": "balance",
                    "Time Off Type DB ID": "timeofftypeuri",
                    "User DB ID": "useruri",
                }
        )

        query_for_user_with_multiple_reocrds_forsame_timeofftype = rail.QueryCollectionOperator(
            task_id="query_for_user_with_multiple_reocrds_forsame_timeofftype",
            query="""SELECT employeeid, timeofftype, balance, COUNT(*) FROM employeetimeoffdata
                    GROUP BY employeeid,timeofftype
                    HAVING count(*) > 1
                    """
        )

        if_user_with_multiple_records = rail.IfOperator(
            task_id = "if_user_with_multiple_records",
            test='{{result("query_for_user_with_multiple_reocrds_forsame_timeofftype")|load_all_records|length>0}}',
            yes_task="write_user_with_multiple_records_log",
            no_task="write_log_csv"
        )

        write_user_with_multiple_records_log = rail.WriteLogOperator(
            task_id="write_user_with_multiple_records_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="Data is obtained from User Time off Balance Details report",
            items='{{result("query_for_user_with_multiple_reocrds_forsame_timeofftype")}}',
            properties= lambda item:{
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"Employee ID - "+ item["employeeid"]+
                            " has multiple time off types assigned with the same category "+
                            item["timeofftype"]+". Hence, this entry will not be exported.",
                "Blank":null
            }
        )
        query_for_user_with_single_timeofftype = rail.QueryCollectionOperator(
            task_id="query_for_user_with_single_timeofftype",
            query="""SELECT employeeid, timeofftype, balance, COUNT(*) FROM employeetimeoffdata
                    GROUP BY employeeid, timeofftype
                    HAVING COUNT(*) < 2
                    """
        )

        if_user_with_single_timeofftype = rail.IfOperator(
            task_id = "if_user_with_single_timeofftype",
            test='{{result("query_for_user_with_single_timeofftype")|load_all_records|length>0}}',
            yes_task="write_upload_data_log",
            no_task="write_no_data_for_user_log"
        )

        write_no_data_for_user_log = rail.WriteLogOperator(
            task_id="write_no_data_for_user_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="No Time Off Balance data found to be exported",
            properties=lambda: json.dumps({
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"No Time Off Balance data found to be exported",
                "Blank":null
            })
        )

        write_upload_data_log = rail.WriteLogOperator(
            task_id="write_upload_data_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="Writing data to output file",
            properties=lambda: json.dumps({
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"Writing data to output file at the location - "+ config.sftp_timeoff_balance_upload_path + "Worker Time Off Balance File Schema.csv",
                "Blank":null
            })
        )

        list_timeoff_export_files = rail.SFTPListFilesOperator(
            task_id='list_timeoff_export_files',
            paths=[config.sftp_timeoff_balance_upload_path]
        )

        has_time_off_balance_export_in_sftp_path = rail.IfOperator(
            task_id="has_time_off_balance_export_in_sftp_path",
            test='{{result("list_timeoff_export_files")|length > 0}}',
            yes_task="archive_timeoff_balance_existing_exports",
            no_task="write_timeoff_balance_export_csv"
        )

        archive_timeoff_balance_existing_exports = rail.ForEachOperator(
            task_id="archive_timeoff_balance_existing_exports",
            items=lambda:rail.result('list_timeoff_export_files').get(config.sftp_timeoff_balance_upload_path),
            start_task="check_for_timeoff_filename_suffix",
            end_task="archive_timeoff_balance_existing_exports_end"
        )

        check_for_timeoff_filename_suffix = rail.IfOperator(
            task_id="check_for_timeoff_filename_suffix",
            test='{{result("archive_timeoff_balance_existing_exports")["name"] | ends_with("Worker Time Off Balance File Schema.csv")|is_truthy}}',
            yes_task="archive_timeoff_export_file",
        )

        archive_timeoff_export_file = rail.SFTPMoveFileOperator(
            task_id="archive_timeoff_export_file",
            existing_filename=config.sftp_timeoff_balance_upload_path + '{{result("archive_timeoff_balance_existing_exports")["name"]}}',
            new_filename=config.sftp_timeoff_balance_archive_path + "Worker Time Off Balance File Schema_" +
                        '{{current_time_in_specified_tz(fmt="%d%m%Y%H%M%S", tz="America/Denver")}}.csv'
        )

        archive_timeoff_balance_existing_exports_end = rail.EmptyOperator(task_id="archive_timeoff_balance_existing_exports_end")

        write_timeoff_balance_export_csv = rail.WriteCSVFileOperator(
            task_id="write_timeoff_balance_export_csv",
            source='{{result("query_for_user_with_single_timeofftype")}}',
            header=None,
            row=lambda item:[
                    item["employeeid"],
                    item["timeofftype"],
                    item["balance"],
                    (now(tz=config.time_zone)+relativedelta(days=-14)).strftime("%Y/%m/%d")
            ],
            delimiter='|'
        )

        upload_timeoff_export_to_sftp= rail.SFTPUploadFileOperator(
            task_id="upload_timeoff_export_to_sftp",
            content='{{result("write_timeoff_balance_export_csv")}}',
            remote_filepath=config.sftp_timeoff_balance_upload_path + "Worker Time Off Balance File Schema.csv"
        )

        write_no_of_records_log = rail.WriteLogOperator(
            task_id="write_no_of_records_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="Total count of time off balance records extracted from WTS as of today",
            properties=lambda: {
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"Total count of time off balance records extracted from WTS as of today are - " +
                            '{{result("create_user_timeoff_collection")|length}}',
                "Blank":null
            }
        )

        write_success_log = rail.WriteLogOperator(
            task_id="write_success_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="Time Off Balance extract file generated successfully",
            properties=lambda: {
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"Time Off Balance extract file generated successfully",
                "Blank":null
            }
        )
        write_completion_log = rail.WriteLogOperator(
            task_id="write_completion_log",
            log='{{result("strayeruniversity_log_lookup_table")}}',
            message="Time off balance extract utility process completed",
            properties=lambda:{
                "Datetimemt":now(tz=config.time_zone).strftime("%m/%d/%Y %H:%M:%S"),
                "Info":"Info",
                "Details":"***** Time off balance extract utility process completed *****",
                "Blank":null
            }
        )
        if_timeoff_export_success = rail.IfOperator(
            task_id="if_timeoff_export_success",
            test='{{get_error_message()| is_falsy}}',
            yes_task="send_timeoff_export_success_mail",
            no_task="send_timeoff_export_failure_mail"
        )
        write_log_csv = rail.WriteCSVFileOperator(
            task_id="write_log_csv",
            source='{{result("strayeruniversity_log_lookup_table")}}',
            header=["datetimemt", "info", "details", "blank"],
            row=[
                '{{item.properties | attr_or_default("Datetimemt", "")}}',
                '{{item.properties | attr_or_default("Info", "")}}',
                '{{item.properties | attr_or_default("Details", "")}}',
                '{{item.properties | attr_or_default("Blank", "")}}'
            ]
        )
        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name='{{result("write_log_csv")}}',
            output_file_name="customer_logfile.txt",
            expires_in_seconds=30*24*60*60
        )

        send_timeoff_export_failure_mail = rail.EmailOperator(
            task_id="send_timeoff_export_failure_mail",
            to=config.alert_email,
            bcc=config.internal_log_emails,
            subject="{{get_company_key()}} | Time Off Balance Extract for Workday has not completed|{{ current_time('%Y-%m-%dT%H:%M:%S.%f%z') }}",
            html_content="templates/error_mail.html"
        )

        send_timeoff_export_success_mail = rail.EmailOperator(
            task_id="send_timeoff_export_success_mail",
            to=config.alert_email,
            subject="{{get_company_key()}} |Time Off Balance Extract for Workday has completed successfully|{{ current_time('%Y-%m-%dT%H:%M:%S.%f%z') }}",
            html_content="templates/success_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger"
        )
        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{get_error_message()| is_truthy}}",
            yes_task="fail_dagrun"
        )
        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        if_fourteen_days_since_last_dag_run >> rail.Label("Yes") >>\
        update_last_execution_date >> strayeruniversity_log_lookup_table >>\
        get_time_off_report_details >> run_report_entry >> run_report_exit >>\
        if_report_generation_successful >> rail.Label("Yes") >>\
        if_timeoff_data_not_present >> rail.Label("Yes") >> write_timeoff_data_found_log >>\
        parse_timeoff_csv >>\
        create_user_timeoff_collection >> query_for_user_with_multiple_reocrds_forsame_timeofftype >>\
        if_user_with_multiple_records >> rail.Label("No") >> write_log_csv
        if_user_with_multiple_records >> rail.Label("Yes") >>\
        write_user_with_multiple_records_log >> write_log_csv
        create_user_timeoff_collection >> query_for_user_with_single_timeofftype >>\
        if_user_with_single_timeofftype >> rail.Label("Yes") >> write_upload_data_log >>\
        list_timeoff_export_files >>\
        has_time_off_balance_export_in_sftp_path >> rail.Label("Yes") >>\
        archive_timeoff_balance_existing_exports>>\
        check_for_timeoff_filename_suffix >> rail.Label("Yes") >>\
        archive_timeoff_export_file >> archive_timeoff_balance_existing_exports_end
        archive_timeoff_balance_existing_exports >> archive_timeoff_balance_existing_exports_end>>\
        write_timeoff_balance_export_csv >> upload_timeoff_export_to_sftp >>write_no_of_records_log >>\
        write_success_log>> write_completion_log >> write_log_csv
        has_time_off_balance_export_in_sftp_path >> rail.Label("No") >> write_timeoff_balance_export_csv
        if_user_with_single_timeofftype >> rail.Label("No") >> write_no_data_for_user_log >> write_log_csv
        if_report_generation_successful >> rail.Label("No") >> write_report_generation_fail_log >> write_log_csv
        if_timeoff_data_not_present >> rail.Label("No") >>  write_no_data_log >> write_log_csv
        write_log_csv >> generate_downloadable_link >>\
        if_timeoff_export_success >> rail.Label("Yes") >> send_timeoff_export_success_mail >> log_to_sumo
        if_timeoff_export_success >> rail.Label("No") >> send_timeoff_export_failure_mail >> log_to_sumo
        if_fourteen_days_since_last_dag_run >> rail.Label("No") >> delete_dag_run
        log_to_sumo>> can_fail_dag >> fail_dagrun
    return dag

rail.for_each_instance(create_airflow_dag)
