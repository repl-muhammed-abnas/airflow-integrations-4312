"""
restore_database.py
-------------------
Child DAG for restoring a single SQL Server .bak backup file.

One instance is triggered per .bak file discovered in the customer's SFTP input directory.
dag_run.conf must include:
    file_path     — full path to the .bak on the Windows SQL Server host
    database_name — target database name (Ajera or VantagePoint, runtime-stamped)
    customer_id   — customer identifier (used to resolve sql_conn_id via load_customer_config)
    instance      — deployment instance (e.g. 'production', 'trial')

Single task: restore_database — renders RESTORE_DATABASE_SQL with Jinja and executes
with autocommit=True (required for DDL/RESTORE statements in pymssql).
"""

from airflow.operators.python import PythonOperator
import rail
from ajera_vantagepoint_migration.sql.common.restore import RESTORE_DATABASE_SQL
from ajera_vantagepoint_migration.utils.custom_methods import render_and_execute_sql, load_customer_config, load_customer_config


def create_restore_dag(config):
    """
    Single child DAG for database restoration.
    Customer-specific config loaded from parent DAG trigger parameters.
    """
    with rail.create_airflow_dag(
        dag_id=f"ajera_vantagepoint_migration_restore_database_{config.instance}",
        description="Restore SQL database from backup file and verify it is online",
        company_key=config.company_key,
        integration_type="generic",
        max_active_runs=10,
        schedule_interval=None,
        catchup=False,
    ) as dag:

        def _restore_database(dag_run):
            """Restore database from backup file using autocommit.

            Retrieves SQL connection ID dynamically from Airflow Variables
            based on customer_id, then executes restore with autocommit=True.
            """
            conf = dag_run.conf or {}
            file_path = conf.get('file_path', '')
            database_name = conf.get('database_name', '')

            config = load_customer_config(dag_run)
            sql_conn_id = config['sql_conn_id']

            render_and_execute_sql(
                conn_id=sql_conn_id,
                params={'file_path': file_path, 'database_name': database_name},
                sql=RESTORE_DATABASE_SQL,
            )


        restore_database = PythonOperator(
            task_id="restore_database",
            python_callable=_restore_database,
        )

    return dag


# Create the single restore DAG
rail.for_each_instance(create_restore_dag)

#remove sql_filepath from methods
