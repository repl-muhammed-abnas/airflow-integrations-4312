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
                                rail.result(f"trigger_parallel_grant_processing_{x+1}")
                                if rail.result(f"trigger_parallel_grant_processing_{x+1}")
                                else []
                            ),
                            range(config.parallel_trigger_count),
                        )
                    )
                )
            ),
        )

        gather_grant_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_grant_logs",
            dag_runs='{{ result("get_dag_run_ids") }}',
            dagrun_task_id="create_log",
            flatten=True,
        )

        def format_logs():
            logs = []
            master_log = [rail.result("create_project_log")] + rail.result("gather_grant_logs") or []
            for log_artifact in master_log:
                grant_logs = rail.load_all_records(log_artifact)
                for log in grant_logs:
                    logs.append({"ecid": log.get("ecid"), "severity": log["severity"] , **(log.get("properties") or {})})
            rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', logs ))))
            rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', logs ))))
            rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', logs ))))
            return logs

        load_all_logs = rail.PythonOperator(
            task_id="load_all_logs",
            python_callable=format_logs,
            show_return_value_in_logs=False,
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_to_csv",
            source=lambda: rail.result('load_all_logs'),
            header=["Grant Name", "Grant Code", "Status","Action", "Details", "JobID"],
            row=[
                '{{ item | attr_or_default("grant_name", "") }}',
                '{{ item | attr_or_default("grant_code", "") }}',
                '{{ item | attr_or_default("status", "") }}',
                '{{ item | attr_or_default("action", "") }}',
                '{{ item | attr_or_default("details", "") }}',
                '{{ item | attr_or_default("ecid", "") }}',
            ],
            footer=['Number of records found:{{ result("create_input_collection" , "length")}}',
                    'Number of records processed:'+'{{- result("load_all_logs", key="exception_record_count") + result("load_all_logs",key="error_record_count")+ \
                        result("load_all_logs", key="success_record_count")}}',
                    'Number of success records: {{ result("load_all_logs", key="success_record_count")}}',
                    'Number of error records: {{ result("load_all_logs", key="error_record_count") }}',
                    'Number of exception records: {{ result("load_all_logs", key="exception_record_count") }}',
                ]
        )

        get_log_filename = rail.PythonOperator(
            task_id="get_log_filename",
            python_callable=lambda:rail.render_template(
                "{{get_company_key()}}_{{ current_time_in_specified_tz(fmt='%Y%m%d_%H%M%S') }}_grant_import_log.csv")
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('write_logs_to_csv')}}",
            output_file_name="{{result('get_log_filename')}}",
            expires_in_seconds=7*24*60*60
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_mail",
            to=config.tenant_email,
            bcc="{%- if result('load_all_logs', key='error_record_count') == 0 -%}"
                + config.internal_logs_email
                + "{%- else -%}"
                + config.alerts_email
                + "{%- endif -%}",
            subject='{{ get_company_key() }} | Replicon Grant Import {{" "}}'
                    '{%- if result("load_all_logs", key="error_record_count") > 0 -%}'
                    ' Completed with errors'
                    '{%- elif result("load_all_logs", key="exception_record_count") > 0 -%}'
                    ' Completed with exceptions'
                    '{%- else -%}'
                    ' Completed Successfully -'
                    '{%- endif -%}'
                    ' {{ " " + current_time_in_specified_tz() }}',
            html_content="templates/send_complete_mail.html",
        )

        (
            get_dag_run_ids
            >> gather_grant_logs
            >> load_all_logs
            >> write_logs_to_csv
            >> get_log_filename
            >> generate_downloadable_link
            >> send_import_complete_email
        )

        return task_group
