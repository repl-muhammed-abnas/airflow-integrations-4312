from datetime import timedelta
import rail
from pwcglobal.user_import_australia.send_logs_allowance_import import get_send_logs


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_allowance_process_each_file_child_{config.instance}",
        description=f"PwCGlobal User Import Australia User Allowance process each file {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")
        create_allowance_log = rail.CreateLogOperator(
            task_id="create_allowance_log"
        )
        is_file_csv = rail.IfOperator(
            task_id="is_file_csv",
            test="{{dag_run.conf.file_name | file_ext | lower == 'csv' }}",
            yes_task="download_file",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath="{{dag_run.conf.file_path}}" +
            "/"+"{{dag_run.conf.file_name}}"
        )

        parse_input_file = rail.LoadCSVFileOperator(
            task_id="parse_input_file",
            document="{{result('download_file')}}",
        )

        input_data_collection = rail.CreateCollectionOperator(
            task_id="input_data_collection",
            source="{{result('parse_input_file')}}",
            columns={
                "Employee ID": "employee_id",
                "GUID": "guid",
                "Compensation Plan Effective Date": "compensation_plan_effective_date",
                "Expected End Date": "expected_end_date",
                "Compensation Element": "compensation_element"
            }
        )

        input_data = rail.QueryCollectionOperator(
            task_id="input_data",
            query="SELECT * FROM input_data_collection"
        )
        send_logs, send_logs_end = get_send_logs(config)

        has_data = rail.IfOperator(
            task_id="has_data",
            test="{{result('input_data', 'length') > 0}}",
            yes_task="process_each_records",
            no_task=send_logs.task_id
        )

        process_each_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_records",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_allowance_child_process_each_records_{config.instance}",
            items="{{result('input_data')}}",
            conf=lambda item, dag_run: {
                "employee_id": item['employee_id'],
                "guid": item["guid"],
                "compensation_plan_effective_date": item["compensation_plan_effective_date"],
                "expected_end_date": item["expected_end_date"],
                "compensation_element": item["compensation_element"],
                "file_name": dag_run.conf['file_name'],
                "log": rail.result("create_allowance_log")
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_process_each_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_records',
            dag_runs='{{ result("process_each_records") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        archive_input_file_start = rail.EmptyOperator(
            task_id="archive_input_file_start"
        )

        upload_to_archive_folder = rail.SFTPUploadFileOperator(
            task_id = "upload_to_archive_folder",
            remote_filepath=config.user_import_archive_path + "{{dag_run.conf.file_name}}",
            content="{{result('download_file')}}",
        )

        delete_the_input_file = rail.SFTPDeleteFileOperator(
            task_id = "delete_the_input_file",
            existing_filename="{{dag_run.conf.file_path}}/{{dag_run.conf.file_name}}"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "file_name": "{{ dag_run.conf.file_name }}",
                "archived_file_name":  "{{dag_run.conf.file_path}}/{{dag_run.conf.file_name}}"
            }
        )
        create_allowance_log >> is_file_csv >> rail.Label("Yes") >> download_file >> parse_input_file >> input_data_collection >> input_data >> has_data \
            >> rail.Label("Yes") >> process_each_records >> wait_for_process_each_records >> send_logs
        has_data >> rail.Label("No") >> send_logs
        download_file >> archive_input_file_start >> upload_to_archive_folder >> delete_the_input_file
        send_logs_end >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
