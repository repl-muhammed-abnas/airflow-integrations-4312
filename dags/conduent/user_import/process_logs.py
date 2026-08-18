from datetime import timedelta
from pendulum import datetime
from conduent.user_import.utils import custom_methods
import rail


def create_airflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.conduent_user_import_process_logs_child,
        description="conduent user import logs",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_master,
        start_date=datetime(2024, 8, 18, tz=config.time_zone),
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        if_user_import_logs = rail.IfOperator(
            task_id="if_user_import_logs",
            test=lambda dag_run: dag_run.conf.get(
                "parent_run_id", "") and not dag_run.conf.get("disable_user", ""),
            yes_task='load_all_logs',
            no_task='get_disable_user_import_logs'
        )

        get_disable_user_import_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_disable_user_import_logs',
            execution_timeout=timedelta(days=config.execution_timeout),
            dag_runs='{{dag_run.conf.parent_run_id}}',
            dagrun_task_id="create_disable_user_log",
            flatten=True
        )

        load_all_logs = rail.PythonOperator(
            task_id='load_all_logs',
            python_callable=custom_methods.format_logs
        )

        if_log_records = rail.IfOperator(
            task_id="if_log_records",
            test=lambda: rail.result("load_all_logs"),
            yes_task="create_user_import_master_log",
            no_task="delete_dag_run"
        )

        delete_dag_run = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dag_run")

        create_user_import_master_log = rail.CreateLogOperator(
            task_id="create_user_import_master_log"
        )

        write_user_import_master_log = rail.WriteLogOperator(
            task_id='write_user_import_master_log',
            log='{{result("create_user_import_master_log")}}',
            items='{{result("load_all_logs")|to_json}}',
            message="aggregating log",
            severity=lambda item: item['status'],
            properties={
                "timestamp": '{{item.timestamp}}',
                "employee_id": '{{ item| attr_or_default("win_id", "") }}',
                "login_name":'{{ item| attr_or_default("login_name", "") }}',
                "employee_first_name": '{{item| attr_or_default("employee_first_name","")}}',
                "employee_last_name": '{{ item | attr_or_default("employee_last_name","")}}',
                "status": '{{item | attr_or_default("status","")}}',
                "action": '{{item | attr_or_default("action","")}}',
                "details": '{{item | attr_or_default("details","")}}',
                "ecid": '{{item.ecid}}'
            }
        )

        filter_user_import_exception = rail.FilterLogEntriesOperator(
            task_id='filter_user_import_exception',
            log='{{result("create_user_import_master_log")}}',
            severity="Exception"
        )

        filter_user_import_failures = rail.FilterLogEntriesOperator(
            task_id='filter_user_import_failures',
            log='{{result("create_user_import_master_log")}}',
            severity="Error"
        )

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=lambda dag_run:rail.render_template("Userimport_Logs_" +\
            '{{ current_time_in_specified_tz(fmt="%d%m%Y_%H%M%S", tz="US/Eastern") }}'+".csv")
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_to_csv',
            source='{{result("write_user_import_master_log")}}',
            header=["Timestamp", 'Employee ID', "Login Name", 'Employee First Name',
                    'Employee Last Name', 'Status', "Action", 'Details', "Jobid"],
            row=['{{ item.properties | attr_or_default("timestamp", "") }}',
                 '{{ item.properties | attr_or_default("employee_id", "") }}',
                 '{{ item.properties | attr_or_default("login_name", "") }}',
                 '{{item.properties | attr_or_default("employee_first_name","")}}',
                 '{{ item.properties | attr_or_default("employee_last_name","")}}',
                 '{{item.properties | attr_or_default("status","")}}',
                 '{{item.properties | attr_or_default("action","")}}',
                 '{{item.properties | attr_or_default("details","")}}',
                 '{{item.properties | attr_or_default("ecid","")}}'],
            footer=lambda:[
                "Number of records found: "+ str(rail.result("load_all_logs","total_count")),
                "Number of records processed: " + str(rail.result("load_all_logs","total_processed_count")),
                "Number of Successes: " + str(rail.result("load_all_logs","success_count")),
                "Number of failures:" + str(rail.result("load_all_logs","error_record_count")),
                "Number of new users added:" + str(rail.result("load_all_logs","user_add_count")),
                "Number of user profiles updated:" + str(rail.result("load_all_logs","user_update_count"))
            ] if rail.result("load_all_logs","total_count") > 0 else []
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_logs_to_sftp",
            content='{{result("write_logs_to_csv")}}',
            remote_filepath=config.sftp_log_path + "/" + '{{result("get_log_file_name")}}'
        )

        generate_pre_signed_download_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_pre_signed_download_url',
            artifact_name='{{result("write_logs_to_csv")}}',
            output_file_name='{{result("get_log_file_name")}}',
            expires_in_seconds=7*24*60*60
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_mail",
            to=config.tenant_mail,
            bcc="{%- if result('filter_user_import_failures', 'length') == 0 -%}\
                    "+config.internal_logs_mail+"\
                {%- else -%}\
                    "+config.alert_mail+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | User import {{" "}} \
                {%- if result("filter_user_import_failures", key="length") > 0 -%} \
                    completed with errors  \
                {%- elif result("filter_user_import_exception", key="length") > 0 -%}\
                    completed with exceptions \
                {%- else -%} \
                    completed Successfully - \
                {%- endif -%} \
                {{ " " + current_time_in_specified_tz(fmt="%d%m%Y_%H%M%S", tz="US/Eastern") }}',
            html_content="templates/user_import_complete_mail.html"
        )

        log_complete = rail.EmptyOperator(
            task_id='log_complete'
        )

        if_user_import_logs >> rail.Label("No") >>\
            get_disable_user_import_logs
        if_user_import_logs >> rail.Label("Yes") >>\
            load_all_logs
        get_disable_user_import_logs >>\
            load_all_logs >> if_log_records >> rail.Label("Yes") >>\
            create_user_import_master_log >> write_user_import_master_log >>\
            filter_user_import_exception >> filter_user_import_failures >>\
            get_log_file_name >>\
            write_logs_to_csv >> upload_logs_to_sftp >> generate_pre_signed_download_url >>\
            send_import_complete_email >> log_complete
        if_log_records >> rail.Label("No") >> delete_dag_run
    return dag


rail.for_each_instance(create_airflow_master_dag)
