"""
process_sp_scripts.py
---------------------
Child DAG for executing the Ajera → VantagePoint SP migration scripts in dependency order.

Triggered by the master DAG with dag_run.conf:
    ajera_db_name — runtime Ajera database name (replaces [PK_Ajera_Demo] placeholder)
    vp_db_name    — runtime VantagePoint database name (replaces [PK_VantagepointDemo20252])
    sql_conn_id   — Airflow MsSql connection ID
    customer_id   — customer identifier
    instance      — deployment instance (e.g. 'production', 'trial')

Execution order:
    [001_list_insert, 002_state_country] → 003_client → 004_client_address
        → 005_vendor → [006_vendor_address, 007_contact]
"""

import rail
from airflow.operators.python import PythonOperator
from ajera_vantagepoint_migration.sql.ajera.storedprocedure_scripts.sql_mapper import sp_sql_map
from ajera_vantagepoint_migration.utils.custom_methods import run_sp_sql_file


def create_sp_dag(config):
    """
    Single child DAG for migration stored procedure scripts.
    Customer-specific config loaded from parent DAG trigger parameters.
    """
    with rail.create_airflow_dag(
        dag_id=f"ajera_vantagepoint_migration_sp_scripts_run_{config.instance}",
        description="Migration SP Scripts Execution DAG",
        company_key=config.company_key,
        integration_type="generic",
        max_active_runs=1,
        schedule_interval=None,
        catchup=False,
    ) as dag:

        def run_sql(dag_run, sql_key):
            """Extract runtime DB names and connection from dag_run.conf."""
            conf = dag_run.conf or {}
            ajera_db_name = conf.get('ajera_db_name', '')
            vp_db_name = conf.get('vp_db_name', '')
            sql_conn_id = conf.get('sql_conn_id', 'sql_ajera_to_vp')


            run_sp_sql_file(
                ajera_db=ajera_db_name,
                vp_db=vp_db_name,
                conn_id=sql_conn_id,
                sql=sp_sql_map[sql_key],
            )

        # SP Script 1: ListInsert
        sp_001_list_insert = PythonOperator(
            task_id="sp_001_list_insert",
            python_callable=run_sql,
            op_kwargs={"sql_key": "001_list_insert"},
        )

        # SP Script 2: StateCountry_Updates
        sp_002_state_country = PythonOperator(
            task_id="sp_002_state_country",
            python_callable=run_sql,
            op_kwargs={"sql_key": "002_state_country"},
        )

        # SP Script 3: cnvClient
        sp_003_client = PythonOperator(
            task_id="sp_003_client",
            python_callable=run_sql,
            op_kwargs={"sql_key": "003_cnv_client"},
        )

        # SP Script 4: cnvClientAddress
        sp_004_client_address = PythonOperator(
            task_id="sp_004_client_address",
            python_callable=run_sql,
            op_kwargs={"sql_key": "004_cnv_client_address"},
        )

        # SP Script 5: cnvVendor
        sp_005_vendor = PythonOperator(
            task_id="sp_005_vendor",
            python_callable=run_sql,
            op_kwargs={"sql_key": "005_cnv_vendor"},
        )

        # SP Script 6: cnvVendorAddress
        sp_006_vendor_address = PythonOperator(
            task_id="sp_006_vendor_address",
            python_callable=run_sql,
            op_kwargs={"sql_key": "006_cnv_vendor_address"},
        )

        # SP Script 7: cnvContact
        sp_007_contact = PythonOperator(
            task_id="sp_007_contact",
            python_callable=run_sql,
            op_kwargs={"sql_key": "007_cnv_contact"},
        )

        # Dependency flow:
        # 001 (VP lookups) and 002 (source state/country updates) are independent — run in parallel
        # 003 needs both 001 (CFGClientType) and 002 (vecCountry standardized)
        # 004 needs 003 (JOINs cnvClient)
        # 005 (cnvVendor) runs after 004; 006 (cnvVendorAddress) needs 005; 007 independent after 004
        [sp_001_list_insert, sp_002_state_country] >> sp_003_client >> sp_004_client_address >> sp_005_vendor >> [sp_006_vendor_address, sp_007_contact]

    return dag


# Create the single SP scripts DAG
rail.for_each_instance(create_sp_dag)
