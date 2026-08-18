from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
import rail


def create_airflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_fieldglass_workorder_import_gsap_workorder_file_to_replicon_child_{config.instance}",
        description="dxctechnology fieldglass gsap,compass workorder import",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 10, 16),
        max_active_runs=config.master_max_active_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        download_import_file = rail.SFTPDownloadFileOperator(
            task_id="download_import_file",
            remote_filepath='{{dag_run.conf.file_path}}'
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            test='{{get_task_state("download_import_file") == "success"}}',
            yes_task="archive_import_file",
            no_task="delete_dagrun"
        )

        archive_import_file = rail.SFTPMoveFileOperator(
            task_id="archive_import_file",
            new_filename=config.sftp_archive_filepath +
            '{{dag_run_ecid()|replace(":","-")}}_'+'{{dag_run.conf.file_path|file_name}}',
            existing_filename='{{dag_run.conf.file_path}}',
            trigger_rule="all_done"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_feed_file',
            no_task='dummy_load_data'
        )

        decrypt_feed_file = rail.PGPDecryptionOperator(
            task_id="decrypt_feed_file",
            source='{{result("download_import_file")}}',
            pgp_conn_id=config.pgp_conn_id
        )

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_feed_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='true').lower()== 'true' else  rail.result('download_import_file'),
            show_return_value_in_logs= False
        )

        load_import_csv = rail.LoadCSVFileOperator(
            task_id="load_import_csv",
            document='{{result("dummy_load_data")}}',
            delimiter="|",
            headers=[
                        "WorkOrderID",
                        "RevisionNumber",
                        "ContingentWorkerID",
                        "WorkOrderStartDate",
                        "WorkOrderEndDate",
                        "WorkOrderStatus",
                        "WorkerFirstName",
                        "WorkerLastName",
                        "CostCenterCode",
                        "BillRateCategory",
                        "BillRate",
                        "RateUnit",
                        "TaskCode",
                        "WO_GHRPersonnelNumber",
                        "WO_CATW",
                        "WO_WorkerType",
                        "FinanceSystem",
                        "cc_CompanyCode",
                    ]
        )

        create_import_data_collection = rail.CreateCollectionOperator(
            task_id="create_import_data_collection",
            source='{{result("load_import_csv")}}',
            name="fieldglass_workorder_import_collection_gsap"
        )

        if_import_data = rail.IfOperator(
            task_id="if_import_data",
            test='{{result("create_import_data_collection", "length") > 0}}',
            yes_task="process_gsap_workorder_import",
            no_task="send_no_records_mail"
        )

        process_gsap_workorder_import = rail.TriggerDagRunOperator(
            task_id="process_gsap_workorder_import",
            trigger_dag_id=f"dxctechnology_fieldglass_workorder_gsap_import_child_{config.instance}",
            conf={
                "file_name": '{{dag_run.conf.file_path|file_name}}'
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=14)
        )

        send_no_records_mail = rail.EmailOperator(
            task_id="send_no_records_mail",
            to=config.tenant_mail,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}} "+"| GSAP Work order data sync to Replicon - No records in file   -  " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/norecords_file.html",
            params={
                "erp": 'GSAP'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        download_import_file >>\
            was_new_file_found >> rail.Label("Yes") >> archive_import_file
        was_new_file_found >> rail.Label("No") >> delete_dagrun
        download_import_file >> can_decrypt_file >> rail.Label("Yes") >>\
        decrypt_feed_file >> dummy_load_data
        can_decrypt_file >> rail.Label("No") >> dummy_load_data >>\
        load_import_csv >> create_import_data_collection >>\
            if_import_data >> rail.Label(
                "Yes") >> process_gsap_workorder_import >> log_to_sumo
        if_import_data >> rail.Label(
            "No") >> send_no_records_mail >> log_to_sumo
        return dag


rail.for_each_instance(create_airflow_master_dag)
