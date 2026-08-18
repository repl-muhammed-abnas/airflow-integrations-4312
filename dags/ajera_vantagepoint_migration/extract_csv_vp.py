"""
extract_csv_vp.py
-----------------
Child DAG for generic CSV extraction from cnv* staging tables in the Ajera database.

One instance is triggered per entry in config.CSV_EXTRACTIONS.
dag_run.conf must include:
    sql_key             — key into sql/Vantagepoint/csv_extraction/csv_extract_queries.SQL_MAP
    output_filename     — output CSV filename (e.g. 'Client.csv')
    ajera_db_name       — Ajera database name (replaces {ajera_db} placeholder in SQL)
    sftp_output_conn_id — Airflow SFTP connection ID for the output path
    sftp_output_path    — remote SFTP directory for the generated CSV

Flow:
    view_conf → extract_data → query_collection → write_csv → upload_to_sftp
"""

import rail
from ajera_vantagepoint_migration.utils.custom_methods import sql_source


def create_extract_csv_generic_dag(config):
    """
    Single child DAG for generic CSV extraction.
    sql_key, output_filename, ajera_db_name, customer_id, instance passed via dag_run.conf.

    Flow:
        extract_data      — runs SQL, writes results as a RAIL collection, returns collection name
        query_collection  — SELECT * from collection (enables WriteCSVFileOperator2 source pattern)
        write_csv         — WriteCSVFileOperator2 writes local CSV, returns local file path
        upload_to_sftp    — SFTPUploadFileOperator uploads local file to SFTP
    """
    with rail.create_airflow_dag(
        dag_id=f"ajera_vantagepoint_migration_extract_csv_VP_{config.instance}",
        description="Generic CSV Extraction DAG (v2) — sql_key and output_filename passed via conf",
        company_key=config.company_key,
        integration_type="generic",
        max_active_runs=5,
        schedule_interval=None,
        catchup=False,
    ) as dag:

        view_conf = rail.ViewDagRunConfOperator(task_id='view_conf')

        extract_data = rail.CreateCollectionOperator(
            task_id='extract_data',
            source=sql_source,
        )

        # Table name is normalize_identifier('extract_data') = 'extract_data'
        query_collection = rail.QueryCollectionOperator(
            task_id='query_collection',
            query='SELECT * FROM extract_data',
        )

        write_csv = rail.WriteCSVFileOperator2(
            task_id='write_csv',
            source='{{ result("query_collection") }}',
        )

        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            content='{{ result("write_csv") }}',
            remote_filepath='{{ dag_run.conf["sftp_output_path"] }}/{{ dag_run.conf["output_filename"] }}',
            sftp_conn_id='{{ dag_run.conf["sftp_output_conn_id"] }}',
        )

        view_conf >> extract_data >> query_collection >> write_csv >> upload_to_sftp

    return dag


# Create the single CSV extraction DAG
rail.for_each_instance(create_extract_csv_generic_dag)