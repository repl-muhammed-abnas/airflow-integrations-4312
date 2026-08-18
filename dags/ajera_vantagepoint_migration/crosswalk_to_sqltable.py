import rail
from airflow.operators.python import PythonOperator
from ajera_vantagepoint_migration.utils.custom_methods import (
    load_sheets_to_sql as _load_sheets_to_sql,
)


def create_excel_to_sql_dag(config):
    """
    Single child DAG for loading Excel crosswalk files into SQL Server.
    Customer-specific config loaded from parent DAG trigger parameters.
    """
    with rail.create_airflow_dag(
        dag_id=f"ajera_vantagepoint_migration_excel_to_sql_{config.instance}",
        description="Read Excel worksheets and load each into a SQL Server table",
        company_key=config.company_key,
        integration_type="generic",
        max_active_runs=1,
        schedule_interval=None,
        catchup=False,
    ) as dag:

        list_crosswalk_files = rail.SFTPListFilesOperator(
            task_id='list_crosswalk_files',
            paths=['{{ dag_run.conf["sftp_crosswalk_path"] }}'],
            sftp_conn_id='{{ dag_run.conf["sftp_output_conn_id"] }}',
            order_by='modtime',
            order_direction='descending',
        )

        download_excel_file = rail.SFTPDownloadFileOperator(
            task_id='download_excel_file',
            sftp_conn_id='{{ dag_run.conf["sftp_output_conn_id"] }}',
            remote_filepath='{{ dag_run.conf["sftp_crosswalk_path"] }}/{{ result("list_crosswalk_files")[dag_run.conf["sftp_crosswalk_path"]][0]["name"] }}',
        )

        def _load_excel_sheets(dag_run):
            conf = dag_run.conf or {}
            sql_conn_id = conf.get('sql_conn_id', 'sql_ajera_to_vp')
            return _load_sheets_to_sql(sql_conn_id, dag_run=dag_run)

        load_sheets_to_sql = PythonOperator(
            task_id='load_sheets_to_sql',
            python_callable=_load_excel_sheets
        )

        list_crosswalk_files >> download_excel_file >> load_sheets_to_sql

    return dag


# Create the single Excel-to-SQL DAG
rail.for_each_instance(create_excel_to_sql_dag)
