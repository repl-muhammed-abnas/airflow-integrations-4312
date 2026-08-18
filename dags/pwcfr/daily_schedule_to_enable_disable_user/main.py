from datetime import timedelta
from pendulum import datetime,now
from pwcfr.daily_schedule_to_enable_disable_user.tasks.report_export import run_user_status_report
import rail

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcfr_daily_schedule_to_execute_enable_disable_user_master_{config.instance}",
        description="pwcfr daily schedule to execite enable or disable user",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023,7,19,tz=config.cest_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        pwcfr_user_enable_disable_logs = rail.CreateLogOperator(
            task_id="pwcfr_user_enable_disable_logs",
        )

        get_sftp_filename = rail.PythonOperator(
            task_id="get_sftp_filename",
            python_callable= lambda: config.log_file_name_prefix + now(tz=config.cest_timezone).strftime('%m_%d_%Y_T%H_%M_%S')+".csv"
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id="get_all_reports",
            endpoint='/services/ReportService1.svc/GetAllReports'
        )

        if_enable_disable_uri_present = rail.IfOperator(
            task_id="if_enable_disable_uri_present",
            test=lambda: rail.find_first_by_attr_and_get_attr(
                        rail.result("get_all_reports"), "displayText", config.enabled_user_report_name, "uri") and
                        rail.find_first_by_attr_and_get_attr(
                        rail.result("get_all_reports"), "displayText", config.disabled_user_report_name, "uri"),
            yes_task="process_users",
            no_task="fail_dagrun"
        )

        process_users = rail.EmptyOperator(
            task_id="process_users"
        )

        create_disabled_users_collection = run_user_status_report(config.disabled_user_report_name)

        query_to_enable_users = rail.QueryCollectionOperator(
            task_id="query_to_enable_users",
            query="""SELECT * FROM User_report_disabledlist WHERE daydiff ='0.00'"""
        )

        enable_users = rail.TriggerDagRunForEachItemOperator(
            task_id="enable_users",
            items='{{result("query_to_enable_users")}}',
            trigger_dag_id=f"pwcfr_daily_schedule_to_execute_enable_disable_user_child_{config.instance}",
            conf=lambda item:{
                    "action":"enable",
                    "schedulename":item["schedulename"],
                    "useruri":item["useruri"],
                    "username":item["username"],
                    "enddate":item["userenddate"],
                    "lookup_table":rail.result("pwcfr_user_enable_disable_logs"),
                    "parent_ecid":rail.render_template('{{dag_run_ecid()}}')
                }
        )

        wait_for_enable_users_dagrun_completion = rail.WaitForDagRunsSensor(
            task_id="wait_for_enable_users_dagrun_completion",
            dag_runs='{{result("enable_users")}}',
            execution_timeout=timedelta(
                days=1)
        )

        create_enabled_users_collection = run_user_status_report(config.enabled_user_report_name)

        query_to_disable_users_without_schedule_frof = rail.QueryCollectionOperator(
            task_id="query_to_disable_users_without_schedule_frof",
            query="""SELECT * FROM User_report_enabledlist WHERE daydiff >= 1 AND schedulename != "FROF" """
        )

        disable_users_without_schedule_frof = rail.TriggerDagRunForEachItemOperator(
            task_id="disable_users_without_schedule_frof",
            items='{{result("query_to_disable_users_without_schedule_frof")}}',
            trigger_dag_id=f"pwcfr_daily_schedule_to_execute_enable_disable_user_child_{config.instance}",
            conf=lambda item:{
                    "action":"disable",
                    "schedulename":item["schedulename"],
                    "useruri":item["useruri"],
                    "username":item["username"],
                    "enddate":item["userenddate"],
                    "lookup_table":rail.result("pwcfr_user_enable_disable_logs"),
                    "parent_ecid":rail.render_template('{{dag_run_ecid()}}')
                }
        )

        wait_for_disable_users_dagrun_completion = rail.WaitForDagRunsSensor(
            task_id="wait_for_disable_users_dagrun_completion",
            dag_runs='{{result("disable_users_without_schedule_frof")}}',
            execution_timeout=timedelta(
                days=1)
        )

        query_to_disable_users_with_schedule_frof = rail.QueryCollectionOperator(
            task_id="query_to_disable_users_with_schedule_frof",
            query="""SELECT * FROM User_report_enabledlist WHERE schedulename="FROF" """
        )

        disable_users_with_schedule_frof = rail.TriggerDagRunForEachItemOperator(
            task_id="disable_users_with_schedule_frof",
            items='{{result("query_to_disable_users_with_schedule_frof")}}',
            trigger_dag_id=f"pwcfr_daily_schedule_to_execute_enable_disable_user_child_{config.instance}",
            conf=lambda item:{
                    "action":"disable",
                    "schedulename":item["schedulename"],
                    "useruri":item["useruri"],
                    "username":item["username"],
                    "enddate":item["userenddate"],
                    "lookup_table":rail.result("pwcfr_user_enable_disable_logs"),
                    "parent_ecid":rail.render_template('{{dag_run_ecid()}}')
                }
        )

        wait_for_disablefrof_users_dagrun_completion = rail.WaitForDagRunsSensor(
            task_id="wait_for_disablefrof_users_dagrun_completion",
            dag_runs='{{result("disable_users_with_schedule_frof")}}',
            execution_timeout=timedelta(
                days=1)
        )

        if_enabled_disabled_user_data = rail.IfOperator(
            task_id="if_enabled_disabled_user_data",
            test=lambda:bool(rail.result('query_to_enable_users') or
                             rail.result("query_to_disable_users_without_schedule_frof") or
                             rail.result("query_to_disable_users_with_schedule_frof")),
            yes_task="if_entries_in_lookup_table",
            no_task="log_to_sumo"
        )

        if_entries_in_lookup_table = rail.IfOperator(
            task_id="if_entries_in_lookup_table",
            test='{{result("pwcfr_user_enable_disable_logs")| load_all_records() |length > 0}}',
            yes_task="filter_for_error_logs",
            no_task="log_to_sumo"
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_to_csv",
            source='{{result("pwcfr_user_enable_disable_logs")}}',
            header=[
                        "username",
                        "action",
                        "enddate",
                        "schedule",
                        "status",
                        "details",
                        "jobid"
                    ],
            row=['{{ item.properties | attr_or_default("username", "") }}', '{{  item.properties | attr_or_default("action", "") }}',
                 '{{ item.properties | attr_or_default("enddate", "") }}', '{{ item.properties | attr_or_default("schedulename", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}', '{{ item.properties | attr_or_default("details", "") }}','{{ dag_run_ecid() }}'],
            lineterminator='\n'
        )

        generate_log_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_log_download_link',
            artifact_name="{{ result('write_logs_to_csv')}}",
            output_file_name='{{result("get_sftp_filename")}}',
            expires_in_seconds=7*24*60*60,
        )

        filter_for_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_for_error_logs",
            log='{{result("pwcfr_user_enable_disable_logs")}}',
            severity="Error"
        )

        if_error_logs = rail.IfOperator(
            task_id="if_error_logs",
            test='{{result("filter_for_error_logs") | load_all_records | length > 0}}',
            yes_task="send_error_mail",
            no_task="send_success_mail"
        )

        send_error_mail = rail.EmailOperator(
            task_id="send_error_mail",
            to=config.alert_email,
            bcc=config.bcc_error_alert_mail,
            # pylint: disable=line-too-long
            subject=f"{config.company_key} | Daily schedule to enable/disable users completed with errors - {now(tz='Europe/Paris').strftime('%Y-%m-%dT%H:%M:%S.%f%z')}",
            html_content='templates/error_mail.html',
        )

        send_success_mail = rail.EmailOperator(
            task_id="send_success_mail",
            to=config.alert_email,
            # pylint: disable=line-too-long
            subject=f"{config.company_key} | Daily schedule to enable/disable users completed successfully - {now(tz='Europe/Paris').strftime('%Y-%m-%dT%H:%M:%S.%f%z')}",
            html_content='templates/success_mail.html',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{get_error_message()|is_truthy}}",
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{get_error_message()}}"
        )

        pwcfr_user_enable_disable_logs >> get_sftp_filename >> get_all_reports >> \
        if_enable_disable_uri_present >> rail.Label("Yes") >> process_users >>\
        create_disabled_users_collection >> query_to_enable_users >> enable_users >>\
        wait_for_enable_users_dagrun_completion >> if_enabled_disabled_user_data
        process_users >> create_enabled_users_collection >> query_to_disable_users_with_schedule_frof >> \
        disable_users_with_schedule_frof >> \
        wait_for_disablefrof_users_dagrun_completion >> if_enabled_disabled_user_data
        create_enabled_users_collection >> query_to_disable_users_without_schedule_frof >> disable_users_without_schedule_frof >>\
        wait_for_disable_users_dagrun_completion >> \
        if_enabled_disabled_user_data
        if_enable_disable_uri_present >> rail.Label("No") >> fail_dagrun
        if_enabled_disabled_user_data >> rail.Label("Yes") >> \
        if_entries_in_lookup_table >> rail.Label("Yes") >> \
        filter_for_error_logs >> write_logs_to_csv >> generate_log_download_link >> \
        if_error_logs >> rail.Label("Yes") >> send_error_mail  >> log_to_sumo
        if_error_logs >> rail.Label("No") >> send_success_mail  >> log_to_sumo
        if_entries_in_lookup_table >> rail.Label("No") >> log_to_sumo
        if_enabled_disabled_user_data >> rail.Label("No") >> log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag

rail.for_each_instance(create_main_airflow_dag)
