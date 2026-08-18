import itertools
import rail


null = None


def process_log_task_group(config):
    with rail.TaskGroup(
        group_id="process_log_generation",
        prefix_group_id=False
    ) as task_group:

        get_dag_run_ids = rail.PythonOperator(
            task_id="get_dag_run_ids",
            python_callable=lambda: list(
                itertools.chain(
                    *list(
                        map(
                            lambda x: (
                                rail.result(f"trigger_parallel_project_processing_{x+1}")
                                if rail.result(
                                    f"trigger_parallel_project_processing_{x+1}"
                                )
                                else []
                            ),
                            range(config.max_active_runs_child),
                        )
                    )
                )
            ),
        )

        gather_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_project_logs",
            dag_runs='{{result("get_dag_run_ids")}}',
            dagrun_task_id="create_log",
            flatten=True,
        )

        def format_logs():
            logs = []
            project_log_artifacts = rail.result("gather_project_logs")
            unchanged_project_log = rail.load_all_records(rail.result("create_master_log"))
            if project_log_artifacts:
                for log_artifact in project_log_artifacts:
                    project_logs = rail.load_all_records(log_artifact)
                    for log in project_logs:
                        logs.append({"ecid": log["ecid"], "severity": log["severity"], **log["properties"]})
            if unchanged_project_log:
                for log in unchanged_project_log:
                    logs.append({"ecid": log["ecid"], "severity": log["severity"], **log["properties"]})
            return logs

        load_all_logs = rail.PythonOperator(
            task_id="load_all_logs",
            python_callable=format_logs,
            show_return_value_in_logs=False,
        )

        create_project_import_master_log = rail.CreateLogOperator(
            task_id="create_project_import_master_log"
        )

        write_project_import_master_log = rail.WriteLogOperator(
            task_id="write_project_import_master_log",
            log='{{result("create_project_import_master_log")}}',
            items='{{result("load_all_logs")|to_json}}',
            message="aggregating log",
            severity=lambda item:item["severity"],
            properties={
                "projectname": "{{item.projectname}}",
                "projectcode": "{{ item.projectcode }}",
                "status": "{{item.status}}",
                "details": "{{item.details}}",
                "ecid": "{{item.ecid}}",
            },
        )

        filter_project_import_exception = rail.FilterLogEntriesOperator(
            task_id="filter_project_import_exception",
            log='{{result("create_project_import_master_log")}}',
            severity="Exception",
        )

        filter_project_import_failures = rail.FilterLogEntriesOperator(
            task_id="filter_project_import_failures",
            log='{{result("create_project_import_master_log")}}',
            severity="Error",
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_to_csv",
            source='{{result("write_project_import_master_log")}}',
            header=["Project Name", "Project Code", "Status", "Details", "JobID"],
            row=[
                '{{ item.properties | attr_or_default("projectname", "") }}',
                '{{item.properties | attr_or_default("projectcode","")}}',
                '{{ item.properties | attr_or_default("status","")}}',
                '{{ item.properties | attr_or_default("details","")}}',
                '{{item.properties | attr_or_default("ecid", "")}}',
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=lambda: rail.render_template(
            "ProjectImport_Logs_" + '{{ current_time_in_specified_tz(fmt="%d%m%Y_%H%M%S", tz="Etc/UTC") }}' + ".csv")
        )

        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_to_sftp",
            content='{{result("write_logs_to_csv")}}',
            remote_filepath=config.logs_filepath + '{{result("get_log_file_name")}}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_mail",
            to=config.tenant_email,
            bcc="{%- if result('filter_project_import_failures', 'length') == 0  -%}\
                    "
            + config.internal_logs_email
            + "\
                {%- else -%}\
                    "
            + config.alerts_email
            + "\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Project Import {{"-"}} \
                {%- if result("filter_project_import_failures", key="length") > 0 -%} \
                     {{" "}}Completed with errors  \
                {%- elif result("filter_project_import_exception", key="length") > 0 -%}\
                     {{" "}}Completed with exceptions \
                {%- else -%} \
                    {{" "}}Completed Successfully - \
                {%- endif -%} \
                {{ " " + current_time("%m-%d-%Y-%H-%M-%S.%f%z") }}',
            html_content="templates/send_completion_mail.html",
            params={
                "file_path": config.logs_filepath,
            },
        )

        (
            get_dag_run_ids
            >> gather_project_logs
            >> load_all_logs
            >> create_project_import_master_log
            >> write_project_import_master_log
            >> filter_project_import_exception
            >> filter_project_import_failures
            >> write_logs_to_csv
            >> get_log_file_name
            >> upload_to_sftp
            >> send_import_complete_email
        )
        return task_group
