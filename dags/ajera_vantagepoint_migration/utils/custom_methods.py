"""
custom_methods.py
-----------------
Shared utility functions for the Ajera → VantagePoint migration pipeline.

Grouped by the DAG that imports each function:
  1. main.py                 — webhook config, DB naming, connection setup,
                               validation, record counts, DB verification
  2. restore_database.py     — Jinja SQL rendering + autocommit execution
  3. process_sp_scripts.py   — SP placeholder substitution and execution
  4. crosswalk_to_sqltable.py — Excel sheet → SQL loading
  5. extract_csv_Vp.py       — SQL → RAIL collection extraction
  6. setup_customer.py       — SFTP connection attrs
"""

import csv
import datetime
import decimal
import os

import numpy as np
import pandas as pd
import rail
from airflow import settings
from airflow.models import Connection, Variable
from rail.hooks.mssql_hook import MsSqlEncryptedHook
from rail.lib.data_reader import DataReader


# ---------------------------------------------------------------------------
# 1. main.py
# ---------------------------------------------------------------------------

def _get_payload(dag_run):
    """Extract the actual payload from dag_run.conf.

    RAIL wraps webhook-triggered payloads as::

        {"webhook": {"data": {...actual payload...}, "headers": {...}, ...}}

    This helper returns the inner ``data`` dict when that wrapper is present,
    or the conf dict directly for manually-triggered DAGs.
    """
    conf = dag_run.conf or {}
    webhook = conf.get('webhook')
    if isinstance(webhook, dict) and 'data' in webhook:
        return webhook['data']
    return conf


def load_customer_config(dag_run):
    """
    Load customer configuration from webhook payload + Airflow Variables.

    Webhook payload must include:
    {
        "customer_id": "acme_corp",
        "instance": "production"  (optional, defaults to "production")
    }

    Connection IDs and paths are derived from customer_id:
    - sftp_conn_id: {customer_id}_sftp_input_{instance}
    - sql_conn_id: {customer_id}_sqlserver_{instance}
    - windows_ssh_conn_id: {customer_id}_windows_ssh_{instance}

    Returns:
        dict: Complete customer configuration including connection IDs, paths, etc.

    Raises:
        ValueError: If customer_id is missing from webhook payload
    """


    payload = _get_payload(dag_run)

    # Extract customer info from webhook payload
    customer_id = payload.get('customer_id')
    if not customer_id:
        raise ValueError(
            "customer_id required in webhook payload. "
            'Payload format: {"customer_id": "acme_corp", "instance": "production"}'
        )

    instance = payload.get('instance', 'production')

    # Build complete configuration
    config = {
        'customer_id': customer_id,
        'instance': instance,

        # === CLIENT INFO (from webhook payload) ===
        'client_name': payload.get('client_name', ''),
        'client_email': payload.get('client_email', ''),
        'connection_attributes': payload.get('connection_attributes', {}),

        # === CONNECTION IDs ===
        # Source SFTP is per-customer (credentials from UI payload)
        # SQL Server, Windows SSH, and output SFTP use shared connections directly
        'sftp_conn_id': f"{customer_id}_sftp_ajera_to_vp_input_{instance}",
        #'vp_instance_conn_id': f"{customer_id}_vp_instance_ajera_to_vp_{instance}",
        'vp_instance_conn_id': "ajera_vantagepoint_migration_vp_instance_conn",
        'sql_conn_id': payload.get('base_sql_conn_id', 'sql_ajera_to_vp'),
        'sql_ajera_conn_id': f"{customer_id}_{instance}_ajera",
        'windows_ssh_conn_id': payload.get('base_ssh_conn_id', 'ssh_ajera_to_vp_apoorv'),
        'sftp_output_conn_id': payload.get('base_sftp_conn_id', 'sftp_ajera_to_vp_integration_useast'),
        # 'mailtrap_conn_id': payload.get('base_mailtrap_conn_id', 'mailtrap_conn'),

        # === PATHS (Customer-Isolated) ===
        #'sftp_input_path': f"/ajera_vantagepoint_migration/input",
        'sftp_input_path': f"/ajera_vp_test_ajera_vantagepoint_migration/backupfiles",
        'sftp_crosswalk_path': f"/ajera_vp_test_ajera_vantagepoint_migration/crosswalk",
        'sftp_output_path': f"/ajera_vantagepoint_migration/{customer_id}/output/csv",
        'sftp_report_path': f"/ajera_vantagepoint_migration/{customer_id}/output/report",
        'destination_base_path': f"G:/SQLBackups/ajera_vantagepoint_migration/{customer_id}",

        # === DB NAME PREFIXES ===
        'ajera_db_prefix': f"{customer_id.upper()}_Ajera",
        'vp_db_prefix': f"{customer_id.upper()}_VP",

        # === NOTIFICATIONS ===
        'data_review_email': Variable.get(
            f"{customer_id}_data_review_email_{instance}",
            default_var="ops@company.com"
        ),
    }


    return config


def ajera_db_name(timestamp):
    """Build the Ajera database name using the config prefix and run timestamp.

    Args:
        timestamp (str): DAG run start time formatted as 'YYYYMMDD_HHMMSS'

    Returns:
        str: e.g. 'ACMECORP_AJ_20250327_143022'
    """
    prefix = rail.result('load_customer_config')['ajera_db_prefix']
    return f"{prefix}_{timestamp}"


def vantagepoint_db_name(timestamp):
    """Build the VantagePoint database name using the config prefix and run timestamp.

    Args:
        timestamp (str): DAG run start time formatted as 'YYYYMMDD_HHMMSS'

    Returns:
        str: e.g. 'ACMECORP_VP_20250327_143022'
    """
    prefix = rail.result('load_customer_config')['vp_db_prefix']
    return f"{prefix}_{timestamp}"




def validate_webhook_params(dag_run):
    """Validate required webhook parameters before any downstream work.

    Raises:
        ValueError: If 'customer_id' is missing from the webhook payload.
    """
    p = _get_payload(dag_run)
    customer_id = p.get('customer_id', '').strip()
    if not customer_id:
        raise ValueError("Parameter 'customer_id' is required!")


def validate_bak_files():
    """Validate that at least one .bak file was found after the SFTP listing.

    Raises:
        ValueError: If no .bak files are present in the SFTP input directory.
    """
    files_by_path = rail.result('list_files')
    bak_files = [
        f
        for files in files_by_path.values()
        for f in files
        if f['name'].lower().endswith('.bak')
    ]
    if not bak_files:
        raise ValueError("No .bak files found in the customer's SFTP input directory")



def verify_restored_databases(sql_conn_id, ajera_db_name, vp_db_name):
    """Verify that both restored databases are ONLINE in SQL Server.

    Queries sys.databases for each database and raises if either is not ONLINE.
    Called in the master DAG after wait_for_restores to confirm both the Ajera
    and VantagePoint databases are ready before the pipeline continues.

    Args:
        sql_conn_id (str): Airflow MsSql connection ID (base, no schema).
        ajera_db_name (str): Restored Ajera database name.
        vp_db_name (str): Restored VantagePoint database name.

    Raises:
        ValueError: If either database is not found or not in ONLINE state.
    """
    hook = MsSqlEncryptedHook(mssql_conn_id=sql_conn_id)
    for db_name in [ajera_db_name, vp_db_name]:
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT state_desc FROM sys.databases WHERE name = %s", (db_name,)
            )
            rows = cursor.fetchall()
        finally:
            conn.close()
        if not rows:
            raise ValueError(f"Restore verification failed: database '{db_name}' not found in sys.databases")
        state = rows[0][0]
        if state != 'ONLINE':
            raise ValueError(f"Restore verification failed: database '{db_name}' is '{state}', expected ONLINE")


# ---------------------------------------------------------------------------
# 2. restore_database.py
# ---------------------------------------------------------------------------

def render_and_execute_sql(conn_id, params, sql):
    """Render a Jinja-templated SQL string and execute with autocommit.

    Designed for DDL-heavy scripts (e.g. RESTORE DATABASE) that require
    autocommit=True to work correctly with pymssql — normal transactions
    would block DDL statements.

    Args:
        conn_id (str): Airflow MsSql connection ID.
        params (dict): Variables exposed inside the template as
                       ``{{ params.<key> }}``.
        sql (str):     SQL template string.

    Raises:
        Exception: Re-raises any database error after printing a diagnostic message.
    """
    from jinja2 import Template

    sql_script = Template(sql).render(params=params)

    hook = MsSqlEncryptedHook(mssql_conn_id=conn_id)
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    try:
        cursor.execute(sql_script)
        # Drain any remaining result sets so the connection closes cleanly
        while cursor.nextset():
            pass
    except Exception:
        raise
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# 3. process_sp_scripts.py
# ---------------------------------------------------------------------------

def run_sp_sql_file(ajera_db, vp_db, conn_id, sql):
    """Substitute DB placeholder names in an SP migration script and execute.

    SP scripts are authored with two literal placeholder database names that
    must be swapped for the real runtime names before execution:

        [Ajera_db]       → [<ajera_db>]
        [Vantagepoint_db] → [<vp_db>]

    Args:
        ajera_db (str): Ajera database name (without brackets), e.g. 'AJ_AjeraDemo'.
        vp_db (str):    VantagePoint database name (without brackets).
        conn_id (str):  Airflow MsSql connection ID.
        sql (str):      SQL string.
    """
    sql = sql.replace('[Ajera_db]', f'[{ajera_db}]')
    sql = sql.replace('[Vantagepoint_db]', f'[{vp_db}]')

    hook = MsSqlEncryptedHook(mssql_conn_id=conn_id)
    hook.run(sql)


# ---------------------------------------------------------------------------
# 4. crosswalk_to_sqltable.py
# ---------------------------------------------------------------------------


def pandas_dtype_to_sql(dtype):
    """Map a pandas dtype to its closest SQL Server column type string.

    Used when dynamically generating CREATE TABLE statements from a DataFrame's
    inferred schema. Falls back to NVARCHAR(MAX) for all other types.

    Args:
        dtype: A pandas dtype object (e.g. ``df['col'].dtype``).

    Returns:
        str: SQL Server type string — one of BIGINT, FLOAT, BIT, DATETIME,
             NVARCHAR(50), or NVARCHAR(MAX).
    """
    if pd.api.types.is_integer_dtype(dtype):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    if pd.api.types.is_bool_dtype(dtype):
        return 'BIT'
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    if pd.api.types.is_timedelta64_dtype(dtype):
        return 'NVARCHAR(50)'
    return 'NVARCHAR(MAX)'


def convert_sql_value(v):
    """Normalise a Python/numpy scalar to a SQL-safe value for pymssql insertion.

    Handles the edge cases that arise when iterating over pandas DataFrame rows:

      - NaN / NaT / None          → None  (SQL NULL)
      - datetime.timedelta         → 'HH:MM:SS' string
      - numpy integer / float / bool → native Python int / float / bool

    Args:
        v: Any scalar value, typically a cell from ``df.itertuples()``.

    Returns:
        A SQL-safe Python scalar, or None to represent SQL NULL.
    """
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, datetime.timedelta):
        total_seconds = int(v.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    return v


def load_sheets_to_sql(sql_conn_id, dag_run):
    """Read every worksheet from the Excel file downloaded by download_excel_file and bulk-load into SQL Server.

    Args:
        sql_conn_id (str): Airflow MsSql connection ID.
        dag_run:           Airflow DagRun (for conf).

    Returns:
        list[str]: Table names that were loaded (e.g. ['xwkSheet1', 'xwkSheet2']).

    Raises:
        ValueError: If dag_run.conf does not include 'database_name'.
    """
    dag_run_conf = dag_run.conf or {}
    database_name = dag_run_conf.get('database_name')
    if not database_name:
        raise ValueError(
            "dag_run.conf must include 'database_name' "
            "(e.g. passed by the master DAG via TriggerDagRunOperator conf)"
        )

    hook = MsSqlEncryptedHook(mssql_conn_id=sql_conn_id)

    loaded_tables = []

    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        with rail.existing_artifact(rail.result('download_excel_file')) as artifact:
            with pd.ExcelFile(artifact.local_filename) as xl:
                for sheet_name in xl.sheet_names:
                    # Detect optional second header row: peek at first 2 rows and check
                    # whether the first data row is entirely strings (sub-header), then
                    # re-read skipping that row so only the main header is used.
                    df_peek = pd.read_excel(xl, sheet_name=sheet_name, header=0, nrows=2)
                    has_second_header = (
                        not df_peek.empty
                        and not df_peek.iloc[0].isna().all()
                        and all(isinstance(v, str) for v in df_peek.iloc[0] if pd.notna(v))
                    )
                    if has_second_header:
                        df = pd.read_excel(xl, sheet_name=sheet_name, header=0, skiprows=[1])
                    else:
                        df = pd.read_excel(xl, sheet_name=sheet_name, header=0)

                    if df.empty:
                        continue

                    table_name = 'xwk' + sheet_name.strip().replace(' ', '_').replace('-', '_')
                    full_table = f"[{database_name}].dbo.[{table_name}]"
                    col_defs = ', '.join(
                        f'[{col}] {pandas_dtype_to_sql(df[col].dtype)}' for col in df.columns
                    )
                    drop_sql = (
                        f"IF OBJECT_ID(N'{full_table}', N'U') IS NOT NULL "
                        f"DROP TABLE {full_table}"
                    )
                    create_sql = f"CREATE TABLE {full_table} ({col_defs})"

                    cols_clause = ', '.join(f'[{c}]' for c in df.columns)
                    placeholders = ', '.join('%s' for _ in df.columns)
                    insert_sql = (
                        f"INSERT INTO {full_table} ({cols_clause}) "
                        f"VALUES ({placeholders})"
                    )

                    rows = [
                        tuple(convert_sql_value(v) for v in row)
                        for row in df.itertuples(index=False, name=None)
                    ]

                    cursor.execute(drop_sql)
                    conn.commit()
                    cursor.execute(create_sql)
                    conn.commit()

                    batch_size = 1000
                    for i in range(0, len(rows), batch_size):
                        cursor.executemany(insert_sql, rows[i:i + batch_size])
                        conn.commit()

                    loaded_tables.append(table_name)
    finally:
        cursor.close()
        conn.close()

    return loaded_tables


# ---------------------------------------------------------------------------
# 5. extract_csv_Vp.py
# ---------------------------------------------------------------------------


def sql_source(dag_run):
    """Callable source for CreateCollectionOperator — executes the keyed SQL query."""
    from ajera_vantagepoint_migration.sql.vantagepoint.csv_extraction.csv_extract_queries import SQL_MAP
    conf = dag_run.conf or {}
    sql_key = conf['sql_key']
    ajera_db = conf['ajera_db_name']
    customer_id = conf.get('customer_id', '')
    instance = conf.get('instance', 'production')
    sql_conn_id = "sql_ajera_to_vp"
    sql = SQL_MAP[sql_key].replace('{ajera_db}', f'[{ajera_db}]')

    hook = MsSqlEncryptedHook(mssql_conn_id=sql_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        headers = [desc[0] for desc in cursor.description]
        rows = [list(row) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

    def _sanitize(v):
        if isinstance(v, decimal.Decimal):
            return float(v)
        return v

    def row_gen():
        for row in rows:
            yield {headers[i]: _sanitize(row[i]) for i in range(len(headers))}

    return DataReader(headers, row_gen())



# ---------------------------------------------------------------------------
# 7. setup_customer.py
# ---------------------------------------------------------------------------

def build_sftp_conn_attrs(dag_run):
    """Build the connection_attributes dict for the per-customer source SFTP connection.

    Reads customer_id, instance, and connection_attributes from dag_run.conf.
    Intended as a callable for CreateAirflowConnection.

    Returns:
        dict: Connection attributes for the sftp conn_id {customer_id}_sftp_input_{instance}.

    Raises:
        ValueError: If connection_attributes is missing from the payload.
    """

    conf = dag_run.conf or {}
    customer_id = conf['customer_id']
    instance = conf.get('instance', 'production')
    attrs = conf.get('connection_attributes', {})
    if not attrs:
        raise ValueError(
            f"[{customer_id}] 'connection_attributes' missing from payload. "
            "The UI must supply SFTP credentials."
        )
    return {
        'conn_id':   f"{customer_id}_sftp_ajera_to_vp_input_{instance}",
        'conn_type': attrs.get('conn_type', 'sftp'),
        'host':      attrs.get('host', ''),
        'login':     attrs.get('login', ''),
        'password':  attrs.get('password', ''),
        'port':      attrs.get('port', 22),
        'extra':     attrs.get('extra', '{}'),
    }





