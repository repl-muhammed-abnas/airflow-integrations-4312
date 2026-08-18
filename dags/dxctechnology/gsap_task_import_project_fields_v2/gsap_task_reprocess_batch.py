from datetime import timedelta, datetime
import pendulum
import rail
from dxctechnology.gsap_task_import_project_fields_v2.utils.python_callable_method import get_process_unique_wbs_conf_reprocess
from dxctechnology.gsap_task_import_project_fields_v2.task.trigger_parallel_dagrun_async import trigger_parallel_dagrun_async
null = None


def create_reprocess_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_project_field_task_import_reprocess_batch_v2_{config.instance}',
        description=f'DXC_sap_project_field_task_import_Reprocess_Batch- V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_conf")

        get_reprocess_update_log = rail.CreateLogOperator(
            task_id='get_reprocess_update_log',
            tenant_wide_name=config.reprocess_wbs_log_name,
            existing_log_mode='append',
        )

        def do_filter_log(log):
            current_time = pendulum.now()
            jobs_created_since = current_time - \
                timedelta(hours=config.first_delta)
            jobs_created_till = current_time - \
                timedelta(hours=config.second_delta)
            timestamp = datetime.strptime(
                log['timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z')

            return jobs_created_till >= timestamp >= jobs_created_since

        filter_log = rail.FilterLogEntriesOperator(
            task_id='filter_log',
            log="{{ result('get_reprocess_update_log')}}",
            filter_callable=do_filter_log,
            remove_filtered_entries=True,
        )

        has_any_data = rail.HasDataOperator(
            task_id='has_any_data',
            source='{{ result("filter_log") }}',
            yes_task='dummy_reprocess_wbs',
            no_task='delete_this_dagrun'
        )

        dummy_reprocess_wbs = rail.EmptyOperator(
            task_id = "dummy_reprocess_wbs"
        )

        def filter_more_than_60_days_entries_callable():
            entries = rail.load_all_records(rail.result("filter_log"))
            reprocess_entries = []
            old_entries = []
            for entry in entries:
                if entry['properties'].get('reprocess_count', 0)//12 >= 60:
                    old_entries.append(entry)
                    continue
                reprocess_entries.append(entry)
            rail.set_result(key="ignored", val=rail.write_json_artifact(old_entries))
            rail.set_result(key="has_ignored_records", val=bool(old_entries))
            rail.set_result(key="has_reprocess_records", val=bool(reprocess_entries))
            return rail.write_json_artifact(reprocess_entries)

        filter_more_than_60_days_entries = rail.PythonOperator(
            task_id = "filter_more_than_60_days_entries",
            python_callable=filter_more_than_60_days_entries_callable
        )

        has_records_to_reprocess = rail.IfOperator(
            task_id = "has_records_to_reprocess",
            test=lambda: rail.result("filter_more_than_60_days_entries", "has_reprocess_records"),
            yes_task="dummy_has_records_to_reprocess_yes_task",
            no_task="dummy_has_records_to_reprocess_no_task"
        )

        dummy_has_records_to_reprocess_no_task = rail.EmptyOperator(
            task_id="dummy_has_records_to_reprocess_no_task"
        )

        has_ignored_records = rail.IfOperator(
            task_id = "has_ignored_records",
            test=lambda: rail.result("filter_more_than_60_days_entries", "has_ignored_records"),
            yes_task="prepare_csv",
        )

        prepare_csv = rail.WriteCSVFileOperator(
            task_id = "prepare_csv",
            source=lambda: rail.result("filter_more_than_60_days_entries", "ignored"),
            header=[
                "FileName",
                "WBS",
            ],
            row=[
                "{{ item.properties.file_name }}",
                "{{ item.properties.wbs }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id = "generate_download_link",
            artifact_name="{{result('prepare_csv')}}",
            output_file_name="WBS_NOT_FOUND_FOR_MORE_THAN_60_DAYS.csv",
            expires_in_seconds=7*24*60*60
        )

        send_email = rail.EmailOperator(
            task_id = "send_email",
            to=config.reprocess_not_found_wbs_email,
            cc=config.internal_logs_email,
            subject="{{ get_company_key() }} | GSAP Task Import - WBS not found for more than 60 days for reprocessing - {{ current_time() }}",
            html_content="templates/emails/email_wbs_not_found.html"
        )

        dummy_has_records_to_reprocess_yes_task = rail.EmptyOperator(
            task_id="dummy_has_records_to_reprocess_yes_task"
        )

        reprocess_wbs = trigger_parallel_dagrun_async(
            task_id='reprocess_wbs',
            parallel_count=10,
            items=lambda: rail.load_json_artifact(rail.result('filter_more_than_60_days_entries')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_each_wbs,
            conf=lambda item, **context: get_process_unique_wbs_conf_reprocess(item, context)
        )

        dummy_reprocess_wbs_complete = rail.EmptyOperator(
            task_id = "dummy_reprocess_wbs_complete"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_reprocess_update_log >> filter_log >> has_any_data
        has_any_data >> rail.Label(
            'Yes') >> dummy_reprocess_wbs >> filter_more_than_60_days_entries >> has_records_to_reprocess >> rail.Label(
                "Yes") >> dummy_has_records_to_reprocess_yes_task >> reprocess_wbs >> dummy_reprocess_wbs_complete >> has_ignored_records
        has_any_data >> rail.Label('No') >> delete_this_dagrun
        has_records_to_reprocess >> rail.Label(
            "No") >> dummy_has_records_to_reprocess_no_task >> has_ignored_records >> rail.Label(
                "Yes") >> prepare_csv >> generate_download_link >> send_email

    return dag


rail.for_each_instance(create_reprocess_dag)
