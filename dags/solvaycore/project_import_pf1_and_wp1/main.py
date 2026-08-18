from datetime import timedelta
from pendulum import datetime
import rail
null=None
dag_created = []
def create_main_airflow_dag(config):
    for source_system in config.project_source_systems:
        with rail.create_airflow_dag(
            dag_id=f"solvaycore_project_import_{source_system}_to_replicon_master_{config.instance}",
            description=f"solvaycore project sync {source_system} to replicon",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            start_date=datetime(2023,7,31,tz=config.cest_time_zone),
            schedule_interval=timedelta(seconds=30),
            max_active_runs=config.max_active_runs_master,
            default_args={
                "sftp_conn_id" : config.sftp_conn_id
            },
        ) as dag:

            new_file_sensor = rail.SFTPAnyFileSensor(
                task_id="new_file_sensor",
                path=config.sftp_input_file_path[source_system],
                soft_fail_timeout=timedelta(minutes=10)
            )

            was_new_file_found = rail.IfOperator(
                task_id="was_new_file_found",
                trigger_rule='all_done',
                test='{{get_task_state("new_file_sensor") == "success"}}',
                yes_task="archive_file",
                no_task="delete_dagrun"
            )

            delete_dagrun = rail.DeleteCurrentDagRunOperator(
                task_id="delete_dagrun"
            )

            if_new_file_is_csv = rail.IfOperator(
                task_id="if_new_file_is_csv",
                test='{{result("new_file_sensor") |file_name|lower| ends_with(".csv")}}',
                yes_task="download_project_import_file",
                no_task="send_incorrect_file_format_mail"
            )

            download_project_import_file = rail.SFTPDownloadFileOperator(
                task_id="download_project_import_file",
                remote_filepath='{{result("new_file_sensor")}}'
            )

            solvaycore_log_lookup_table = rail.CreateLogOperator(
                task_id="solvaycore_log_lookup_table"
            )

            parse_project_import_csv=rail.LoadCSVFileOperator(
                task_id="parse_project_import_csv",
                document='{{result("download_project_import_file")}}'
            )

            create_project_collection = rail.CreateCollectionOperator(
                task_id="create_project_collection",
                source='{{result("parse_project_import_csv")}}',
                name="projectstoimport",
                columns={
                        "Project Code": "projectcode",
                        "Project Description": "projectdescription",
                        "Company Code": "companycode",
                        "Project Status": "projectstatus",
                        "Project Group": "projectgroup",
                        "Origin System": "originsystem",
                        "Controlling Area": "controllingarea",
                        "WBS Code": "wbscode",
                        "WBS Description": "wbsdescription",
                        "SAP Project Type": "sapprojecttype",
                        "WBS Status": "wbsstatus",
                        "SAP WBS Status": "sapwbsstatus",
                        "Project ID": "accoladeprojectid",
                        "Cost Type": "costtype",
                        "Object Class": "objectclass",
                        "WEGO Project Number": "wegoprojectnr",
                        "PS Family": "psfamily",
                        "Project Leader": "projectleader",
                        "GBU": "gbu",
                        "BU": "bu"
                    }
            )

            process_each_project = rail.trigger_parallel_dagrun(
                task_id="process_each_project",
                trigger_dag_id=f"solvaycore_project_import_to_replicon_process_each_project_child_{config.instance}",
                items='{{result("create_project_collection")}}',
                conf=lambda item: {
                    **item,
                    "lookuptable" : rail.result("solvaycore_log_lookup_table"),
                    "parent_ecid": rail.render_template('{{ecid()}}')
                },
                parallel_count=config.max_active_runs_child,
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            if_log_entries_present = rail.IfOperator(
                task_id="if_log_entries_present",
                test='{{result("solvaycore_log_lookup_table")| load_all_records| length> 0}}',
                yes_task="filter_for_failure_messages",
                no_task="finish"
            )

            filter_for_failure_messages = rail.FilterLogEntriesOperator(
                task_id="filter_for_failure_messages",
                severity="Failed",
                log='{{result("solvaycore_log_lookup_table")}}',
            )

            filter_for_exception_messages = rail.FilterLogEntriesOperator(
                task_id="filter_for_exception_messages",
                severity="Exception",
                log='{{result("solvaycore_log_lookup_table")}}'
            )

            write_logs_to_csv = rail.WriteCSVFileOperator(
                task_id="write_logs_to_csv",
                source='{{result("solvaycore_log_lookup_table")}}',
                header=[
                        "Project Code",
                        "Project Description",
                        "JobID",
                        "Task Code",
                        "Status",
                        "Reason",
                        "Child jobid"
                    ],
                row=['{{ item.properties | attr_or_default("projectcode", "") }}', '{{  item.properties | attr_or_default("projectdescription", "") }}',
                 '{{ item.properties | attr_or_default("JobID", "") }}', '{{ item.properties | attr_or_default("Task Code", "") }}',
                 '{{ item.properties | attr_or_default("Status", "") }}', '{{ item.properties | attr_or_default("Reason", "") }}',
                 '{{ item.properties | attr_or_default("Child jobid", "") }}'],
                lineterminator='\n'
            )

            get_file_name = rail.PythonOperator(
                task_id="get_file_name",
                python_callable=lambda ss=source_system:
                config.sftp_log_file_upload_path+rail.render_template("{{ecid()}}").replace(':', '-') + '_' +
                rail.render_template('{{current_time_in_specified_tz(tz="Europe/Dublin", fmt="%m%d%YT%H%M%S")}}') +
                config.project_import_log_filename[ss] + ".csv"
            )

            sftp_project_import_logs_upload = rail.SFTPUploadFileOperator(
                task_id="sftp_project_import_logs_upload",
                content='{{result("write_logs_to_csv")}}',
                remote_filepath="{{result('get_file_name')}}"
            )

            if_failure_messages = rail.IfOperator(
                task_id="if_failure_messages",
                test="{{result('filter_for_failure_messages') | load_all_records | length > 0}}",
                yes_task="send_error_project_import_mail",
                no_task="if_exception_logs"
            )

            send_error_project_import_mail = rail.EmailOperator(
                task_id="send_error_project_import_mail",
                to=config.tenant_email,
                bcc=config.alert_email,
                #pylint:disable=line-too-long
                subject='{{get_company_key()}} | Project Import for {{get_company_key()}} completed with error for '+ source_system.upper()+'  at {{current_time_in_specified_tz(tz="Europe/Dublin", fmt="%m%d%YT%H%M%S")}}',
                html_content="templates/log_email.html",
                params={
                    'source_system': source_system.upper()
                }
            )

            if_excpetion_logs =  rail.IfOperator(
                task_id="if_exception_logs",
                test="{{result('filter_for_exception_messages') | load_all_records | length > 0}}",
                yes_task="send_exception_project_import_mail",
                no_task="send_success_project_import_mail"
            )

            send_exception_project_import_mail = rail.EmailOperator(
                task_id="send_exception_project_import_mail",
                to=config.tenant_email,
                bcc=config.internal_log_emails,
                # pylint:disable=line-too-long
                subject='{{get_company_key()}} | Project Import for {{get_company_key()}} completed with exceptions for '+ source_system.upper()+' at {{current_time_in_specified_tz(tz="Europe/Dublin", fmt="%m%d%YT%H%M%S")}}',
                html_content="templates/log_email.html",
                params={
                    'source_system': source_system.upper()
                }
            )

            send_success_project_import_mail = rail.EmailOperator(
                task_id="send_success_project_import_mail",
                to=config.tenant_email,
                bcc=config.internal_log_emails,
                # pylint:disable=line-too-long
                subject='{{get_company_key()}} | Project Import for {{get_company_key()}} completed successfully for '+ source_system.upper()+' at {{current_time_in_specified_tz(tz="Europe/Dublin", fmt="%m%d%YT%H%M%S")}}',
                html_content="templates/log_email.html",
                params={
                    'source_system': source_system.upper()
                }
            )

            send_incorrect_file_format_mail = rail.EmailOperator(
                task_id="send_incorrect_file_format_mail",
                to=config.tenant_email,
                # pylint:disable=line-too-long
                subject='{{get_company_key()}} | Replicon project import for C1 WBS- Incorrect File Format '+ source_system.upper() +' - {{current_time_in_specified_tz(tz="Europe/Dublin", fmt="%m%d%YT%H%M%S")}}',
                html_content="templates/incorrect_file_format.html"
            )

            archive_file = rail.SFTPMoveFileOperator(
                task_id='archive_file',
                trigger_rule='all_done',
                existing_filename='{{ result("new_file_sensor") }}',
                new_filename=config.sftp_archive_file_path +
                "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
            )

            finish = rail.EmptyOperator(
                task_id="finish"
            )
            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id="log_to_sumo",
                sumo_conn_id="sumologic-dagrunlogger",
                trigger_rule="all_done"
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


            new_file_sensor >>\
            if_new_file_is_csv >> rail.Label("Yes") >> download_project_import_file >> rail.Label("Always") >> \
            was_new_file_found >> rail.Label("Yes") >> archive_file
            download_project_import_file >> solvaycore_log_lookup_table >>\
            parse_project_import_csv >> \
            create_project_collection >> process_each_project >>\
            if_log_entries_present >> rail.Label("Yes") >> filter_for_failure_messages >>\
            filter_for_exception_messages >> write_logs_to_csv >>\
            get_file_name >> sftp_project_import_logs_upload >> \
            if_failure_messages >> rail.Label("Yes") >> send_error_project_import_mail >> log_to_sumo
            if_failure_messages >> rail.Label("No") >> \
            if_excpetion_logs >> rail.Label("Yes") >> send_exception_project_import_mail >> log_to_sumo
            if_excpetion_logs >> rail.Label("No") >> send_success_project_import_mail >> log_to_sumo
            if_log_entries_present >> rail.Label("No") >> finish >> log_to_sumo
            if_new_file_is_csv >> rail.Label("No") >> send_incorrect_file_format_mail >> \
            log_to_sumo >> can_fail_dag >> fail_dagrun
            was_new_file_found >> rail.Label("No") >> delete_dagrun

        dag_created.append(dag)
    return dag_created

rail.for_each_instance(create_main_airflow_dag)
