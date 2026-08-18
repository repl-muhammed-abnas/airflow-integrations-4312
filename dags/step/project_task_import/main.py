from datetime import timedelta
from pendulum import datetime
import rail


def create_ariflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"step_project_task_import_to_replicon_master_{config.instance}",
        description="step project/job task/ticket import to replicon",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 9, 5, tz="PST8PDT"),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_master_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.sftp_import_file_path,
            soft_fail_timeout=timedelta(minutes=config.sftp_time_out)
        )

        was_new_file_found = rail.IfOperator(
            task_id="if_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success" }}',
            yes_task="archive_file",
            no_task="delete_dagrun"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath='{{result("new_file_sensor")}}'
        )

        step_project_or_task_import_lookup_table = rail.CreateLogOperator(
            task_id="step_project_or_task_import_lookup_table"
        )

        get_import_file_name = rail.PythonOperator(
            task_id="get_import_file_name",
            python_callable=lambda: rail.result(
                "new_file_sensor").split('/')[-1]
        )

        download_from_address_file = rail.SFTPDownloadFileOperator(
            task_id="download_from_address_file",
            remote_filepath=config.sftp_from_address_file_path +
            '{{result("new_file_sensor") | file_name | replace(".csv",".txt")}}'
        )

        load_mail_address = rail.PythonOperator(
            task_id="load_mail_address",
            python_callable=lambda: rail.read_artifact(
                rail.result("download_from_address_file"))
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            trigger_rule="all_done",
            existing_filename='{{result("new_file_sensor")}}',
            new_filename=config.sftp_archive_file_path +
            '{{result("new_file_sensor")|file_name}}'
        )

        if_project_import = rail.IfOperator(
            task_id="if_project_import",
            test='{{result("new_file_sensor")| file_name | ends_with("Jobs_Integration.csv")|is_truthy}}',
            yes_task="load_project_import_csv",
            no_task="if_task_import"
        )

        load_project_import_csv = rail.LoadCSVFileOperator(
            task_id="load_project_import_csv",
            document='{{result("download_file")}}',
            headers=["Project Name", "Status", "Percent Completed", "Allow Time Entry", "Project Code",
                     "Project Description", "Start Date", "End Date", "Program Name", "Project Manager",
                     "Project Leader Approval Required", "Invoice Currency", "Estimated Hrs",
                     "Estimated Cost Currency", "Estimated Cost Amount", "Custom Field Supervisor"]
        )

        if_project_data_to_import = rail.IfOperator(
            task_id="if_project_data_to_import",
            test='{{result("load_project_import_csv") | load_all_records| length > 0}}',
            yes_task="step_project_import_start",
            no_task="send_project_import_nodata_mail"
        )

        step_project_import_start = rail.EmptyOperator(
            task_id="step_project_import_start")
        step_project_import = rail.trigger_parallel_dagrun(
            task_id="step_project_import",
            items='{{result("load_project_import_csv")}}',
            trigger_dag_id=f"step_project_task_import_to_replicon_create_project_child_{config.instance}",
            parallel_count=config.max_active_child_run,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                **item,
                "lookuptable": rail.result("step_project_or_task_import_lookup_table"),
                "parent_ecid": rail.render_template('{{ecid()}}'),
                "filename": rail.result("get_import_file_name")
            }
        )

        send_project_import_nodata_mail = rail.EmailOperator(
            task_id="send_project_import_nodata_mail",
            to='{{result("load_mail_address")}}',
            subject='{{get_company_key()}} | Project/Jobs import - blank file received Job created at Properties {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/nodata_mail.html",
            params={
                "import_type": "Project/Jobs"
            }
        )

        filter_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_error_logs",
            log='{{result("step_project_or_task_import_lookup_table")}}',
            severity="Error"
        )

        write_project_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_to_csv",
            source='{{result("step_project_or_task_import_lookup_table")}}',
            header=["projectname", "status", "details"],
            row=['{{ item.properties.projectname }}',
                 '{{ item.properties.status }}',
                 '{{ item.properties.childjobid}}'+"|" + '{{item.properties.details}}',]
        )

        generate_pre_signed_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_pre_signed_download_link",
            artifact_name='{{result("write_logs_to_csv")}}',
            output_file_name='{{result("new_file_sensor")|file_name}}_importlog_{{current_time("%Y-%m-%dT%H%M%S%z")}}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_project_import_complete_mail = rail.EmailOperator(
            task_id="send_project_import_complete_mail",
            to='{{result("load_mail_address")}}',
            subject='{{ get_company_key() }} | Project/Job import {{" "}} \
                {%- if result("filter_error_logs", key="length") > 0 -%} \
                completed with errors  \
                {%- else -%} \
                completed Successfully - \
                {%- endif -%} \
                {{current_time("%Y-%m-%dT%H%M%S%z")}}',
            html_content="templates/project_import_success.html",
        )

        if_task_import = rail.IfOperator(
            task_id="if_task_import",
            test='{{result("new_file_sensor")| file_name | ends_with("Ticket_Integration.csv")|is_truthy}}',
            yes_task="load_task_import_csv",
            no_task="log_to_sumo"
        )

        load_task_import_csv = rail.LoadCSVFileOperator(
            task_id="load_task_import_csv",
            document='{{result("download_file")}}'
        )

        if_task_data_to_import = rail.IfOperator(
            task_id="if_task_data_to_import",
            test='{{result("load_task_import_csv") | load_all_records| length > 0}}',
            yes_task="step_task_import_start",
            no_task="send_task_import_nodata_mail"
        )
        step_task_import_start = rail.EmptyOperator(
            task_id="step_task_import_start")

        step_task_import = rail.trigger_parallel_dagrun(
            task_id="step_task_import",
            items='{{result("load_task_import_csv")}}',
            trigger_dag_id=f"step_project_task_import_to_replicon_create_task_child_{config.instance}",
            parallel_count=config.max_active_child_run,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                **item,
                "lookuptable": rail.result("step_project_or_task_import_lookup_table"),
                "parent_ecid": rail.render_template('{{ecid()}}'),
                "filename": rail.result("get_import_file_name")
            }
        )
        step_task_import_end = rail.EmptyOperator(
            task_id="step_task_import_end")
        write_task_log_to_csv = rail.WriteCSVFileOperator(
            task_id="write_task_log_to_csv",
            source='{{result("step_project_or_task_import_lookup_table")}}',
            header=["projectname", "taskname", "status", "details"],
            row=['{{ item.properties.projectname }}',
                 '{{item.properties.Taskname}}',
                 '{{ item.properties.status }}',
                 '{{ item.properties.childjobid}}'+"|" + '{{item.properties.details}}']
        )

        generate_pre_signed_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_pre_signed_url",
            artifact_name='{{result("write_task_log_to_csv")}}',
            output_file_name='{{result("new_file_sensor")|file_name}}_importlog_{{current_time("%Y-%m-%dT%H%M%S%z")}}.csv',
            expires_in_seconds=7*24*60*60
        )

        filter_for_task_creation_errors = rail.FilterLogEntriesOperator(
            task_id="filter_for_task_creation_errors",
            log='{{result("step_project_or_task_import_lookup_table")}}',
            severity="Error"
        )

        send_task_import_complete_mail = rail.EmailOperator(
            task_id="send_task_import_complete_mail",
            to='{{result("load_mail_address")}}',
            subject='{{ get_company_key() }} | Task/Ticket import {{" "}} \
                {%- if result("filter_for_task_creation_errors", key="length") > 0 -%} \
                completed with errors  \
                {%- else -%} \
                completed Successfully - \
                {%- endif -%} \
                {{current_time("%Y-%m-%dT%H%M%S%z")}}',
            html_content="templates/task_success_mail.html",
        )

        send_task_import_nodata_mail = rail.EmailOperator(
            task_id="send_task_import_nodata_mail",
            to='{{result("load_mail_address")}}',
            subject='{{get_company_key()}} | Task/Ticket import - blank file received Job created at Properties {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/nodata_mail.html",
            params={
                "import_type": "Task/Ticket"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )
        
        download_file >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_dagrun
        new_file_sensor >> get_import_file_name >> download_file >> step_project_or_task_import_lookup_table >>\
            download_from_address_file >> load_mail_address >>\
            if_project_import >> rail.Label("Yes") >>\
            load_project_import_csv >>\
            if_project_data_to_import >> rail.Label("Yes") >> step_project_import_start >> step_project_import >>\
            filter_error_logs>>\
            write_project_logs_to_csv >> generate_pre_signed_download_link >>\
            send_project_import_complete_mail >> log_to_sumo
        if_project_data_to_import >> rail.Label(
            "No") >> send_project_import_nodata_mail >> log_to_sumo
        if_project_import >> rail.Label("No") >> if_task_import
        if_task_import >> rail.Label("Yes") >>\
            load_task_import_csv >>\
            if_task_data_to_import >> rail.Label(
                "Yes") >> step_task_import_start >> step_task_import >> step_task_import_end >>\
            write_task_log_to_csv >> generate_pre_signed_url >>\
            filter_for_task_creation_errors >>\
            send_task_import_complete_mail >> log_to_sumo
        if_task_data_to_import >> rail.Label(
            "No") >> send_task_import_nodata_mail >> log_to_sumo
        if_task_import >> rail.Label("No") >> log_to_sumo
        log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag


rail.for_each_instance(create_ariflow_master_dag)
