from datetime import timedelta
from pendulum import datetime
from americanintegrated.task_import.tasks.create_task_type_collection import create_task_entry_collection
from americanintegrated.task_import.tasks.create_reference_task_type_collection import create_reference_task_entry_collection

import rail
null = None

# pylint: disable=too-many-statements

def create_airflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"amerincanintergrated_task_import_for_existing_projects_master_{config.instance}",
        description="task import for exisitng projects",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2023, 9, 28, tz=config.pacific_time_zone),
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        americanintegrated_task_lookup_table = rail.CreateLogOperator(
            task_id="americanintegrated_task_lookup_table"
        )

        create_latest_basic_task_collection = create_task_entry_collection(
            config.sftp_task_file_path, "basic_task_latest")

        list_sftp_reference_files = rail.SFTPListFilesOperator(
            task_id="list_sftp_reference_files_basic_task",
            paths=[config.sftp_task_reference_path],
        )

        if_basic_task_reference_files = rail.IfOperator(
            task_id="if_basic_task_reference_files",
            test='{{result("list_sftp_reference_files_basic_task") |length >0}}',
            yes_task="start_reference_basic_task_collection",
            no_task="fail_dagrun"
        )

        start_reference_basic_task_collection = rail.EmptyOperator(
            task_id="start_reference_basic_task_collection")

        create_reference_basic_task_collection = create_reference_task_entry_collection(
            config.sftp_task_reference_path, "basic_task_reference")

        query_basic_tasks_to_process = rail.QueryCollectionOperator(
            task_id="query_basic_tasks_to_process",
            query="""SELECT * FROM file_content_basic_task_latest WHERE md_5 NOT IN
                    (SELECT DISTINCT  md_5 FROM reference_file_content_basic_task_reference)""",
        )

        create_latest_prevailing_task_collection = create_task_entry_collection(
            config.sftp_wages_file_path, "prevailing_wage_latest")

        list_sftp_prevailing_task_reference_files = rail.SFTPListFilesOperator(
            task_id="list_sftp_reference_files_prevailing_task",
            paths=[config.sftp_wages_reference_path],
        )

        if_prevailing_task_reference_files = rail.IfOperator(
            task_id="if_prevailing_task_reference_files",
            test='{{result("list_sftp_reference_files_prevailing_task") |length >0}}',
            yes_task="start_reference_revailing_task_collection",
            no_task="fail_dagrun"
        )

        start_reference_prevailing_task_collection = rail.EmptyOperator(
            task_id="start_reference_revailing_task_collection")

        create_reference_prevailing_task_collection = create_reference_task_entry_collection(
            config.sftp_wages_reference_path, "prevailing_wage_reference")

        query_prevailing_wage_tasks_to_process = rail.QueryCollectionOperator(
            task_id="query_prevailing_wage_tasks_to_process",
            query="""SELECT * FROM file_content_prevailing_wage_latest WHERE taskcode NOT IN
                    (SELECT DISTINCT taskcode FROM reference_file_content_prevailing_wage_reference)""",
        )

        if_new_prevaling_wage_or_basic_task = rail.IfOperator(
            task_id="if_new_prevaling_wage_or_basic_task",
            test='{{ result("query_prevailing_wage_tasks_to_process", "length") > 0 or\
            result("query_basic_tasks_to_process","length") > 0}}',
            yes_task="get_all_reports",
            no_task="send_no_new_tasks_mail"
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id="get_all_reports",
            endpoint="/services/ReportService1.svc/GetAllReports",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                config.task_assignment_report,
                "uri"
            )
        )

        get_all_custom_fields_for_task = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_for_task",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                    "objectUri": "urn:replicon:object-type:task"
            },
            data_handler=lambda response: {
                "prevailing_wage_dt": rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    "Prevailing wages DT",
                    "uri"),
                "prevailing_wage_ot":rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    "Prevailing wages OT",
                    "uri"),
                "prevailing_wage_rt": rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    "Prevailing wages RT",
                    "uri")
            }
        )

        if_no_task_assignment_report = rail.IfOperator(
            task_id="if_no_task_assignment_report",
            test='{{result("get_all_reports") | is_truthy}}',
            yes_task="run_report",
            no_task="fail_dagrun"
        )

        run_report = rail.EmptyOperator(task_id="run_report")

        run_task_assignment_report_entry, wait_for_task_assignment_report = rail.run_report(
            group_id="run_task_assigment_report",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_all_reports')}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )
        if_report_run_error = rail.IfOperator(
            task_id="if_report_run_error",
            test='{{result("run_task_assigment_report.get_report_result").reportGenerationResults[0].error|is_truthy}}',
            yes_task="fail_dagrun",
            no_task="if_report_has_data"
        )

        if_report_has_data = rail.IfOperator(
            task_id="if_report_has_data",
            test='{{result("run_task_assigment_report.get_report_result", "has_data")}}',
            yes_task="parse_report_data_csv",
            no_task="send_no_inprogress_projects_mail"
        )

        parse_report_data_csv = rail.LoadCSVFileOperator(
            task_id="parse_report_data_csv",
            document='{{result("run_task_assigment_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        process_projects = rail.trigger_parallel_dagrun(
            task_id="process_projects",
            items='{{result("parse_report_data_csv")}}',
            trigger_dag_id=f"amerincanintergrated_task_import_for_existing_projects_child_{config.instance}",
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item: {
                "projectname": item["Project Name"],
                "projecturi": item["uri"],
                "prevailing_wage": item["Prevailing Wage"],
                "Prevailing wages RT uri": rail.result("get_all_custom_fields_for_task")["prevailing_wage_rt"],
                "Prevailing wages OT uri": rail.result("get_all_custom_fields_for_task")["prevailing_wage_ot"],
                "Prevailing wages DT uri": rail.result("get_all_custom_fields_for_task")["prevailing_wage_dt"],
                "lookuptable": rail.result("americanintegrated_task_lookup_table"),
                "parent_ecid": rail.render_template('{{ecid()}}'),
                "basic_tasks_artifact": rail.result("query_basic_tasks_to_process"),
                "basic_tasks_artifact_length": rail.result("query_basic_tasks_to_process", "length"),
                "prevailing_wage_artifact": rail.result("query_prevailing_wage_tasks_to_process"),
                "prevailing_wage_artifact_length": rail.result("query_prevailing_wage_tasks_to_process", "length")
            }
        )

        send_no_inprogress_projects_mail = rail.EmailOperator(
            task_id="send_no_inprogress_projects_mail",
            to=config.tenant_email,
            subject="{{get_company_key()}}" + " | Task assignment for existing projects - " +
            '{{ current_time_in_specified_tz(fmt="%m-%d-%Y") }}',
            html_content="templates/inprogress_project_mail.html"
        )

        send_no_new_tasks_mail = rail.EmailOperator(
            task_id="send_no_new_tasks_mail",
            to=config.tenant_email,
            subject="{{get_company_key()}}" + " | Task assignment for existing projects -  " +
            '{{ current_time_in_specified_tz(fmt="%m-%d-%Y") }}',
            html_content="templates/no_new_tasks_mail.html"
        )

        if_basic_task_input_file = rail.IfOperator(
            task_id="if_basic_task_input_file",
            test='{{result("list_sftp_files_for_basic_task_latest") | length > 0}}',
            yes_task="archive_current_reference",
            no_task="if_prevailing_wage_task_input_file"
        )

        archive_current_reference = rail.SFTPMoveFileOperator(
            task_id="archive_current_reference",
            new_filename=config.sftp_task_archive_path +
            '{{dag_run_ecid()}}' +
            '{{result("for_each_file_basic_task_reference").name}}',
            existing_filename=config.sftp_task_reference_path +
            '{{result("for_each_file_basic_task_reference").name}}'
        )

        compose_basic_task_new_reference_csv = rail.WriteCSVFileOperator(
            task_id="compose_basic_task_new_reference_csv",
            source='{{result("create_content_csv_md5_basic_task_latest")}}'
        )

        upload_basic_task_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_basic_task_new_reference_file",
            content='{{result("compose_basic_task_new_reference_csv")}}',
            remote_filepath=config.sftp_task_reference_path + "newreference.csv"
        )

        if_prevailing_wage_task_input_file = rail.IfOperator(
            task_id="if_prevailing_wage_task_input_file",
            test='{{result("list_sftp_files_for_prevailing_wage_latest") | length > 0}}',
            yes_task="archive_current_wage_reference",
            no_task="log_to_sumo"
        )

        archive_current_wage_reference = rail.SFTPMoveFileOperator(
            task_id="archive_current_wage_reference",
            new_filename=config.sftp_wages_archive_path +
            '{{dag_run_ecid()}}' +
            '{{result("for_each_file_prevailing_wage_reference").name}}',
            existing_filename=config.sftp_wages_reference_path +
            '{{result("for_each_file_prevailing_wage_reference").name}}'
        )

        compose_wage_new_reference_csv = rail.WriteCSVFileOperator(
            task_id="compose_wage_new_reference_csv",
            source='{{result("parse_csv_prevailing_wage_latest")}}'
        )

        upload_wage_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_wage_new_reference_file",
            content='{{result("compose_wage_new_reference_csv")}}',
            remote_filepath=config.sftp_wages_reference_path + "newreference.csv"
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_to_csv",
            source='{{result("americanintegrated_task_lookup_table")}}',
            header=["Job ID",
                    "Child Job ID",
                    "Project Name",
                    "Tasks",
                    "Status",
                    "Details"
                    ],
            row=[
                '{{item.ecid}}',
                '{{item.properties| attr_or_default("child_jobid","")}}',
                '{{item.properties| attr_or_default("project_name","")}}',
                '{{item.properties| attr_or_default("tasks","")}}',
                '{{item.properties| attr_or_default("status","")}}',
                '{{item.properties| attr_or_default("details","")}}',
            ]
        )

        filter_for_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_for_error_logs",
            severity="Error",
            log='{{result("americanintegrated_task_lookup_table")}}'
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_logs_to_sftp",
            content='{{result("write_logs_to_csv")}}',
            remote_filepath=config.sftp_log_file_path +
            "taskassignmentlogs_"+'{{current_time_in_specified_tz(fmt="%m-%d-%Y", tz="US/Pacific")}}' +
                    '_{{dag_run_ecid()}}'+".csv"
        )

        send_import_complete_mail = rail.EmailOperator(
            task_id="send_import_complete_mail",
            to=config.tenant_email,
            bcc="{%- if result('filter_for_error_logs', 'length') > 0 -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Task assignment for existing projects is " }} \
                {%- if result("filter_for_error_logs")| load_all_records() | length > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/import_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger"
        )
        can_faildag = rail.IfOperator(
            task_id="can_faildag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        americanintegrated_task_lookup_table >>\
            create_latest_basic_task_collection >>\
            list_sftp_reference_files >>\
            if_basic_task_reference_files >> rail.Label("Yes") >>\
            start_reference_basic_task_collection >> create_reference_basic_task_collection >> query_basic_tasks_to_process >>\
            create_latest_prevailing_task_collection >> list_sftp_prevailing_task_reference_files >>\
            if_prevailing_task_reference_files >> rail.Label("Yes") >> start_reference_prevailing_task_collection >>\
            create_reference_prevailing_task_collection >> query_prevailing_wage_tasks_to_process >>\
            if_new_prevaling_wage_or_basic_task >> rail.Label(
                "No") >> send_no_new_tasks_mail >> log_to_sumo
        if_new_prevaling_wage_or_basic_task >> rail.Label("Yes") >>\
            get_all_reports >> get_all_custom_fields_for_task >> \
            if_no_task_assignment_report >> rail.Label("No") >> run_report >>\
            run_task_assignment_report_entry >> wait_for_task_assignment_report >>\
            if_report_run_error >> rail.Label("Yes") >> fail_dagrun
        if_report_run_error >> rail.Label("No") >>\
            if_report_has_data >> rail.Label("Yes") >> parse_report_data_csv >> process_projects >>\
            if_basic_task_input_file >> rail.Label(
            "Yes") >> archive_current_reference >> compose_basic_task_new_reference_csv >>\
            upload_basic_task_new_reference_file >> if_prevailing_wage_task_input_file
        if_basic_task_input_file >> rail.Label("No") >> if_prevailing_wage_task_input_file >> rail.Label("Yes") >>\
            archive_current_wage_reference >> compose_wage_new_reference_csv >> upload_wage_new_reference_file >>\
            filter_for_error_logs >> write_logs_to_csv >> upload_logs_to_sftp >>\
            send_import_complete_mail >> log_to_sumo
        if_prevailing_wage_task_input_file >> rail.Label(
            "No") >> log_to_sumo
        if_report_has_data >> rail.Label(
            "No") >> send_no_inprogress_projects_mail >> log_to_sumo
        if_basic_task_reference_files >> rail.Label("No") >> fail_dagrun
        if_prevailing_task_reference_files >> rail.Label("Yes") >> fail_dagrun
        if_no_task_assignment_report >> rail.Label("Yes") >> fail_dagrun
        log_to_sumo >> can_faildag >> fail_dagrun

        return dag


rail.for_each_instance(create_airflow_master_dag)
