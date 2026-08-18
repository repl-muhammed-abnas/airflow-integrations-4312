"""
main.py
-------
Master orchestrator DAG for the Ajera → VantagePoint database migration.

Creates a single webhook-triggered DAG (ajera_vantagepoint_migration_master) that:
  1. Validates the incoming webhook payload (customer_id required).
  2. Loads per-customer configuration from the payload + Airflow Variables.
  3. Triggers setup_customer to create the per-customer SFTP connection + directories.
  4. Lists .bak files from the customer's SFTP input path.
  5. Downloads and uploads each .bak to the Windows SQL Server host.
  6. Triggers one restore child DAG per .bak file (parallel).
  7. Waits for restores, verifies both databases are ONLINE.
  8. Creates cnv* staging tables and loads the Excel crosswalk (parallel).
  9. Runs SP migration scripts (001/002 → 003 → 004 → 005 → 006/007).
  10. Triggers one CSV extraction child DAG per entry in CSV_EXTRACTIONS (parallel).
"""

import rail
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from ajera_vantagepoint_migration.config import csv_extractions
from ajera_vantagepoint_migration.sql.ajera.table_creation_script.table_create_queries import table_sql_map
from ajera_vantagepoint_migration.utils.custom_methods import (
    ajera_db_name,
    vantagepoint_db_name,
    load_customer_config as _load_customer_config,
    verify_restored_databases,
    validate_webhook_params as _validate_webhook_params,
    validate_bak_files as _validate_bak_files,
    _get_payload,
)



def create_migration_dag(config):
    """
    Single dynamic DAG for all customers.
    Customer config loaded from webhook payload + Airflow Variables.
    No code changes needed when adding new customers.
    """

    with rail.create_airflow_dag(
        dag_id=f"ajera_vantagepoint_migration_master_{config.instance}",
        description="Database migration from Ajera to VantagePoint (multi-customer)",
        company_key=config.company_key,
        integration_type="generic",
        schedule_interval=None,  # Webhook-triggered
        webhook_conf=rail.WebhookConf(bearer_token_var="Ajeera_VP_Migration_token"),
        max_active_runs=10,  # Allow multiple customers to run in parallel
        catchup=False,
    ) as dag:

        # 1) View webhook payload for debugging
        view_webhook_payload = rail.ViewDagRunConfOperator(
            task_id='view_webhook_payload'
        )

        # Batch 1: run setup chain inline to avoid scheduler context-switching overhead
        #          covers tasks 1a → 4a (validate_webhook_params through validate_bak_files)
        batch_setup = rail.BatchTaskRunOperator(
            task_id='batch_setup',
            start_task='validate_webhook_params',
            end_task='wait_for_csv_extractions',
        )

        # 1a) Validate required webhook parameters before any downstream work
        validate_webhook_params = PythonOperator(
            task_id='validate_webhook_params',
            python_callable=_validate_webhook_params,
        )

        # 2) Load customer configuration from webhook + Airflow Variables
        load_customer_config = PythonOperator(
            task_id='load_customer_config',
            python_callable=_load_customer_config,
        )

        # 2a) Compute Ajera and VP DB names once from customer config + run timestamp
        #     All downstream triggers read from this XCom instead of recomputing
        compute_db_names = PythonOperator(
            task_id='compute_db_names',
            python_callable=lambda dag_run: {
                'ajera_db_name': ajera_db_name(dag_run.start_date.strftime('%Y%m%d')),
                'vp_db_name': vantagepoint_db_name(dag_run.start_date.strftime('%Y%m%d')),
            },
        )

        # 3) Trigger setup DAG — creates all 5 connections + customer directories
        trigger_setup = rail.TriggerDagRunOperator(
            task_id='trigger_setup',
            trigger_dag_id=f"ajera_vantagepoint_migration_setup_customer_{config.instance}",
            conf=lambda dag_run: (lambda p: {
                'customer_id': p.get('customer_id'),
                'instance': p.get('instance', 'production'),
                'connection_attributes': p.get('connection_attributes', {}),
                'base_sql_conn_id': p.get('base_sql_conn_id', 'sql_ajera_to_vp'),
                'base_ssh_conn_id': p.get('base_ssh_conn_id', 'ssh_ajera_to_vp_apoorv'),
                'base_sftp_conn_id': p.get('base_sftp_conn_id', 'sftp_ajera_to_vp_integration_useast'),
            })(_get_payload(dag_run)),
        )

        vp_conn_test = rail.TriggerDagRunOperator(
            task_id = 'vp_conn_test',
            trigger_dag_id=f"ajera_vantagepoint_migration_vp_conn_test_{config.instance}",
            conf=lambda dag_run : (lambda p: {
                "vp_conn_id" :  p.get('vp_instance_conn_id', "ajera_vantagepoint_migration_vp_instance_conn")
            })(_get_payload(dag_run))

        )

        # 3a) Wait for setup DAG to finish before listing SFTP files
        wait_for_setup = rail.WaitForDagRunsSensor(
            task_id='wait_for_setup',
            dag_runs='{{ result("trigger_setup") }}',
            poke_interval=10,
        )

        # 4) List ALL files in customer's SFTP input directory
        list_files = rail.SFTPListFilesOperator(
            task_id='list_files',
            sftp_conn_id='{{ result("load_customer_config").sftp_conn_id }}',
            paths=['{{ result("load_customer_config").sftp_input_path }}']
        )

        # 4a) Validate at least one .bak file was found before spawning child DAGs
        validate_bak_files = PythonOperator(
            task_id='validate_bak_files',
            python_callable=_validate_bak_files,
        )

        # 5) For each .bak file: download from SFTP then upload to Windows server
        foreach_file = rail.ForEachOperator(
            task_id='foreach_file',
            items=lambda: [
                f
                for files in rail.result('list_files').values()
                for f in files
                if f['name'].lower().endswith('.bak')
            ],
            start_task='download_file',
            end_task='foreach_end'
        )

        # 5a) Download .bak from source SFTP to Airflow worker temp dir
        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id='{{ result("load_customer_config").sftp_conn_id }}',
            remote_filepath='{{ result("load_customer_config").sftp_input_path }}/{{ result("foreach_file")["name"] }}',

        )

        # 5b) Upload from Airflow worker to Windows server via SFTP
        upload_file = rail.SFTPUploadFileOperator(
            task_id='upload_file',
            content='{{ result("download_file") }}',
            remote_filepath='{{ result("load_customer_config").destination_base_path }}/{{ result("foreach_file")["name"] }}',
            sftp_conn_id='{{ result("load_customer_config").windows_ssh_conn_id }}',
        )

        # 5c) Mark end of the foreach loop
        foreach_end = rail.EmptyOperator(task_id='foreach_end')

       # 6) Trigger one restore child DAG per .bak file — runs in parallel
        trigger_restores = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_restores',
            items=lambda: [
                f
                for files in rail.result('list_files').values()
                for f in files
                if f['name'].lower().endswith('.bak')
            ],
            trigger_dag_id=f"ajera_vantagepoint_migration_restore_database_{config.instance}",
            conf=lambda item, dag_run: (lambda p: {
                "file_path": f"{rail.result('load_customer_config')['destination_base_path']}/{item['name']}",
                "database_name": (
                    rail.result('compute_db_names')['vp_db_name']
                    if ('vp' in item['name'].lower() or 'vantagepoint' in item['name'].lower())
                    else rail.result('compute_db_names')['ajera_db_name']
                ),
                "customer_id": p.get('customer_id'),
                "instance": p.get('instance', 'production'),
                "base_sql_conn_id": p.get('base_sql_conn_id', 'sql_ajera_to_vp'),
            })(_get_payload(dag_run))
        )

        # 7) Wait for all restore child DAGs to finish before continuing
        wait_for_restores = rail.WaitForDagRunsSensor(
            task_id='wait_for_restores',
            dag_runs='{{ result("trigger_restores") }}',
            poke_interval=15
        )

        # 7a) Verify both restored databases are ONLINE before continuing
        verify_databases = PythonOperator(
            task_id='verify_databases',
            python_callable=lambda ti: verify_restored_databases(
                sql_conn_id=ti.xcom_pull(task_ids='load_customer_config')['sql_conn_id'],
                ajera_db_name=ti.xcom_pull(task_ids='compute_db_names')['ajera_db_name'],
                vp_db_name=ti.xcom_pull(task_ids='compute_db_names')['vp_db_name'],
            ),
        )

        # 13) Trigger Excel-to-SQL crosswalk load (sequential before table creation)
        trigger_excel_to_sql = rail.TriggerDagRunOperator(
            task_id='trigger_excel_to_sql',
            trigger_dag_id=f"ajera_vantagepoint_migration_excel_to_sql_{config.instance}",
            conf=lambda dag_run: (lambda p: {
                "database_name": rail.result('compute_db_names')['ajera_db_name'],
                "customer_id": p.get('customer_id'),
                "instance": p.get('instance', 'production'),
                "sftp_crosswalk_path": rail.result('load_customer_config')['sftp_crosswalk_path'],
                "sftp_output_conn_id": rail.result('load_customer_config')['sftp_output_conn_id'],
                "sql_conn_id": rail.result('load_customer_config')['sql_conn_id'],
            })(_get_payload(dag_run)),
        )

        # 14) Create cnv* staging tables directly in the Ajera DB
        all_tables_crtn = SQLExecuteQueryOperator(
            task_id="all_tables_crtn",
            conn_id='{{ result("load_customer_config")["sql_conn_id"] }}',
            sql=['USE [{{ result("compute_db_names")["ajera_db_name"] }}];'] + list(table_sql_map.values()),
        )

        # 13a) Wait for Excel-to-SQL to complete (table creation runs inline in parallel)
        wait_for_excel = rail.WaitForDagRunsSensor(
            task_id='wait_for_excel',
            dag_runs='{{ result("trigger_excel_to_sql") }}',
            poke_interval=15
        )

        # 15) Trigger stored procedure migration scripts
        trigger_sp_scripts = rail.TriggerDagRunOperator(
            task_id='trigger_sp_scripts',
            trigger_dag_id=f"ajera_vantagepoint_migration_sp_scripts_run_{config.instance}",
            conf=lambda dag_run: (lambda p: {
                "ajera_db_name": rail.result('compute_db_names')['ajera_db_name'],
                "vp_db_name": rail.result('compute_db_names')['vp_db_name'],
                "customer_id": p.get('customer_id'),
                "instance": p.get('instance', 'production'),
                "sql_conn_id": rail.result('load_customer_config')['sql_conn_id'],
            })(_get_payload(dag_run)),
        )

        # 15a) Wait for SP scripts to complete
        wait_for_sp_scripts = rail.WaitForDagRunsSensor(
            task_id='wait_for_sp_scripts',
            dag_runs='{{ result("trigger_sp_scripts") }}',
            poke_interval=15
        )

        # 16) Trigger generic CSV extraction DAG once per output file
        trigger_csv_extractions = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_csv_extractions',
            items=csv_extractions,
            trigger_dag_id=f"ajera_vantagepoint_migration_extract_csv_VP_{config.instance}",
            conf=lambda item, dag_run: (lambda p: {
                **item,
                "ajera_db_name": rail.result('compute_db_names')['ajera_db_name'],
                "customer_id": p.get('customer_id'),
                "instance": p.get('instance', 'production'),
                "sftp_output_conn_id": rail.result('load_customer_config')['sftp_output_conn_id'],
                "sftp_output_path": rail.result('load_customer_config')['sftp_output_path'],
            })(_get_payload(dag_run)),
        )

        # 16a) Wait for all CSV extraction child DAGs to complete
        wait_for_csv_extractions = rail.WaitForDagRunsSensor(
            task_id='wait_for_csv_extractions',
            dag_runs='{{ result("trigger_csv_extractions") }}',
            poke_interval=15
        )

        # Workflow definition
        view_webhook_payload >> batch_setup >> validate_webhook_params
        validate_webhook_params >> load_customer_config >> compute_db_names >> trigger_setup >> wait_for_setup
        wait_for_setup >> vp_conn_test >> list_files >> validate_bak_files >> foreach_file
        # ForEachOperator pattern: loop body + direct connection for post-loop chaining
        foreach_file >> download_file >> upload_file >> foreach_end
        foreach_file >> foreach_end
        foreach_end >> trigger_restores >> wait_for_restores >> verify_databases \
         >> all_tables_crtn >> trigger_excel_to_sql >> wait_for_excel \
            >> trigger_sp_scripts >> wait_for_sp_scripts >> trigger_csv_extractions
        trigger_csv_extractions >> wait_for_csv_extractions

        batch_setup >> wait_for_csv_extractions



        return dag


# Create the single dynamic DAG
rail.for_each_instance(create_migration_dag)
