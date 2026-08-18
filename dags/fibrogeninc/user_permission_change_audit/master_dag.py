
from datetime import timedelta
from pendulum import datetime
from fibrogeninc.user_permission_change_audit.utils import custom_methods
from fibrogeninc.user_permission_change_audit.utils import request_payload
from fibrogeninc.user_permission_change_audit.tasks.send_logs import get_send_logs
import rail
from airflow.models import Variable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'fibrogeninc_user_update_permission_change_master_{config.instance}',
        description=f'Fibrogeninc user update permission change - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 4, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.get_logging_details
        )

        get_bucket_name = rail.PythonOperator(
            task_id='get_bucket_name',
            python_callable = lambda: Variable.get(
                config.bucket_name, default_var='replicon-airflow-dev-group')
        )

        get_all_custom_fields=rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_report_generate_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="is_report_has_expected_columns"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task="report_has_data",
            no_task="fail_no_expected_columns",
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order does not match'''
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_current_users_csv',
            no_task='finish'
        )

        load_current_users_csv = rail.LoadCSVFileOperator(
            task_id='load_current_users_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        current_users_data = rail.CreateCollectionOperator(
            task_id='current_users_data',
            source='{{ result("load_current_users_csv") }}',
            name='current_users_data'
        )

        list_reference_files = rail.S3ListKeysOperator(
            task_id='list_reference_files',
            bucket_name='{{ result("get_bucket_name") }}',
            prefix=config.reference_key_name,
            aws_conn_id=config.aws_conn_id
        )

        get_file_name = rail.PythonOperator(
            task_id='get_file_name',
            python_callable=custom_methods.get_reference_file_name
        )

        download_reference_file_from_s3 = rail.S3DownloadFileOperator(
            task_id='download_reference_file_from_s3',
            bucket_name='{{ result("get_bucket_name") }}',
            key_name=config.reference_key_name + "/{{ result('get_file_name') }}",
            aws_conn_id=config.aws_conn_id
        )

        load_reference_users_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_users_csv",
            document="{{ result('download_reference_file_from_s3') }}",
        )

        create_reference_users_data = rail.CreateCollectionOperator(
            task_id='create_reference_users_data',
            source = "{{ result('load_reference_users_csv') }}",
            name = "reference_users_data"
        )

        query_changed_records = rail.QueryCollectionOperator(
            task_id='query_changed_records',
            query="""SELECT UserUri, Permission_Name FROM current_users_data
                    EXCEPT
                    SELECT UserUri, Permission_Name FROM reference_users_data""",
            name='changed_records'
        )

        is_changed_records_exists = rail.IfOperator(
            task_id='is_changed_records_exists',
            test='{{ result("query_changed_records", "length") > 0 }}',
            yes_task='query_changed_records_reference_values_from_base_report',
            no_task='move_file_to_s3_archive'
        )

        query_changed_records_reference_values_from_base_report=rail.QueryCollectionOperator(
            task_id='query_changed_records_reference_values_from_base_report',
            query="""SELECT * FROM current_users_data WHERE UserUri IN (SELECT DISTINCT UserUri FROM changed_records)""",
        )

        process_changed_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_changed_records',
            items='{{ result("query_changed_records_reference_values_from_base_report") }}',
            trigger_dag_id=f'fibrogeninc_user_permission_change_update_child_{config.instance}',
            conf=request_payload.get_process_changed_records_payload
        )

        wait_for_process_changed_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_changed_recordss',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_changed_records") }}'
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        move_file_to_s3_archive = rail.S3MoveFileOperator(
            task_id='move_file_to_s3_archive',
            source_bucket_name='{{ result("get_bucket_name") }}',
            existing_key_name=config.reference_key_name + '/{{ result("get_file_name") }}',
            new_key_name=config.archive_key_name + '/{{ result("get_file_name") }}',
            aws_conn_id=config.aws_conn_id
        )

        upload_file_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_file_to_s3',
            source='{{ result("load_current_users_csv") }}',
            key_name=config.reference_key_name + '/{{ result("get_logging_details").filename }}',
            bucket_name='{{ result("get_bucket_name") }}',
            aws_conn_id=config.aws_conn_id,
            replace=True
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        get_logging_details >> get_bucket_name >> get_all_custom_fields >> get_report_details >> run_report_entry
        run_report_exit >> is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> is_report_has_expected_columns

        is_report_has_expected_columns >> rail.Label("Yes") >> report_has_data
        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns

        report_has_data >> rail.Label("Yes") >> load_current_users_csv >> current_users_data >> list_reference_files \
            >> get_file_name >> download_reference_file_from_s3 >> load_reference_users_csv >> create_reference_users_data \
                >> query_changed_records >> is_changed_records_exists
        report_has_data >> rail.Label("No") >> finish

        is_changed_records_exists >> rail.Label("Yes") >> query_changed_records_reference_values_from_base_report \
            >> process_changed_records >> wait_for_process_changed_records >> send_logs_enter
        send_logs_end >> move_file_to_s3_archive >> upload_file_to_s3

        is_changed_records_exists >> rail.Label("No") >> move_file_to_s3_archive >> upload_file_to_s3

    return dag

rail.for_each_instance(create_dag)
