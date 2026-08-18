"""
### Airflow Metadata DB Cleanup (v2)

Chunked, retention-based cleanup of the Airflow metadata database, with a
before -> after email every run so you can see exactly what was cleaned.

Key behaviours:
- **Weekly** schedule (Saturday) so cleanup runs when integration DAGs are idle
  and never impacts weekday runs.
- **Retention** comes from the global `airflow_db_cleanup__max_db_entry_age_in_days`
  Variable (default 30), same as before.
- **Relies on ON DELETE CASCADE.** `dag_run` has cascading FKs, so deleting a
  dag_run auto-deletes its `task_instance` rows and everything under them
  (`xcom`, `task_fail`, `rendered_task_instance_fields`, `task_map`,
  `task_reschedule`, ...). Those children are therefore NOT cleaned here — the
  `dag_run` task handles them via cascade. We only clean tables that do NOT
  cascade from dag_run: `dag_run` itself, `log`, `job`, `session`, `trigger`.
- Per-chunk `ANALYZE` keeps the planner from switching to a bad plan mid-delete.
- **Always emails** a before -> after stats table (success) or the error
  (failure), via rail.EmailOperator. Subject carries [region-env]; recipient is
  the `db_maintenance_alert_email` Variable. This is how you confirm it actually
  cleaned (rows before -> after, incl. the cascade drop on task_instance/xcom).
"""
from datetime import datetime, timedelta
import logging
import os

import airflow
from airflow.settings import engine
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy import text
import rail

log = logging.getLogger(__name__)

# Dedicated DB-maintenance alert recipient. Set the Variable `db_maintenance_alert_email`.
ALERT_EMAIL = "{{ var.value.db_maintenance_alert_email }}"


def _region_env():
    return f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'dev')}"


def _fetch(sql, **params):
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        return conn.execute(text(sql), params).mappings().all()


def _sz(b):
    b = float(b or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} PB"


def _snapshot():
    """Per-table rows + sizes (JSON-serializable) for the before/after email."""
    rows = _fetch(
        """
        SELECT relname,
               n_live_tup AS rows,
               round(100.0*n_dead_tup/nullif(n_live_tup+n_dead_tup,0),1) AS dead_pct,
               pg_relation_size(relid)       AS heap_bytes,
               pg_indexes_size(relid)        AS index_bytes,
               pg_total_relation_size(relid) AS total_bytes
        FROM pg_stat_user_tables
        WHERE pg_total_relation_size(relid) > 8192
        ORDER BY pg_total_relation_size(relid) DESC
        """
    )
    return [{"relname": r["relname"], "rows": int(r["rows"] or 0),
             "dead_pct": float(r["dead_pct"] or 0), "heap_bytes": int(r["heap_bytes"] or 0),
             "index_bytes": int(r["index_bytes"] or 0), "total_bytes": int(r["total_bytes"] or 0)}
            for r in rows]


def _diff_html(before, after, label, note):
    bmap = {x["relname"]: x for x in before}
    body = ""
    for a in after:
        b = bmap.get(a["relname"], {})
        rb, ra = b.get("rows", 0), a["rows"]
        tb, ta = b.get("total_bytes", 0), a["total_bytes"]
        body += (f"<tr><td>{a['relname']}</td>"
                 f"<td align=right>{rb:,} &rarr; {ra:,} ({ra-rb:+,})</td>"
                 f"<td align=right>{a['dead_pct']}</td>"
                 f"<td align=right>{_sz(tb)} &rarr; {_sz(ta)}</td></tr>")
    return f"""<html><body>
    <h3>Airflow DB Cleanup — {label}</h3>
    <p>{note}</p>
    <table border="1" cellpadding="4" cellspacing="0">
      <tr><th>table</th><th>rows (before&rarr;after / deleted)</th><th>dead%</th>
          <th>total size (before&rarr;after)</th></tr>
      {body}
    </table>
    </body></html>"""


with airflow.DAG(
    dag_id='system_airflow_db_cleanup_v2',
    schedule='0 12 * * 6',  # run weekly on Saturdays at 12 PM UTC
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,  # never let a slow run overlap the next trigger
    tags=['system_maintenance'],
    doc_md=__doc__,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
    },
) as dag:

    def chunked_delete(table_name, date_column, id_col, group_by_col, max_date, chunk_size, session):
        while True:
            if group_by_col:
                # Use ROW_NUMBER() to keep the last record in each group
                query = f"""
                    DELETE FROM {table_name}
                    WHERE {id_col} IN (
                        SELECT {id_col} FROM (
                            SELECT {id_col}, ROW_NUMBER() OVER (
                                PARTITION BY {group_by_col}
                                ORDER BY {date_column} DESC
                            ) AS row_num
                            FROM {table_name}
                            WHERE {date_column} < '{max_date}'
                        ) subquery
                        WHERE row_num > 1
                        LIMIT {chunk_size}
                    )
                    RETURNING {id_col};
                """
            else:
                # Standard deletion without grouping
                query = f"""
                    DELETE FROM {table_name}
                    WHERE {id_col} IN (
                        SELECT {id_col} FROM {table_name}
                        WHERE {date_column} < '{max_date}'
                        LIMIT {chunk_size}
                    )
                    RETURNING {id_col};
                """

            try:
                result = session.execute(query)
                rowcount = result.rowcount
                if rowcount == 0:
                    break
                session.commit()
                session.execute(f"ANALYZE {table_name}")
                logging.info(f"Deleted {rowcount} records from {table_name}")
            except Exception as e:
                logging.error(f"Error cleaning up {table_name}: {str(e)}")
                session.rollback()

    @provide_session
    def cleanup_table(table, date_col, id_col, group_by_col, session=NEW_SESSION):
        logging.info(f"Cleaning {table}")
        from airflow.models import Variable
        DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 30
        DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS_VAR_NAME = "airflow_db_cleanup__max_db_entry_age_in_days"
        max_db_entry_age_in_days = int(
            Variable.get(
                DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS_VAR_NAME,
                DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS))

        DEFAULT_CHUNK_SIZE = 5000
        DEFAULT_CHUNK_SIZE_VAR_NAME = "airflow_db_cleanup_chunk_size"
        chunk_size = int(
            Variable.get(
                DEFAULT_CHUNK_SIZE_VAR_NAME,
                DEFAULT_CHUNK_SIZE
            ))

        max_date = timezone.utcnow() + timedelta(-max_db_entry_age_in_days)
        logging.info("Configurations:")
        logging.info("max_db_entry_age_in_days: " +
                     str(max_db_entry_age_in_days))
        logging.info("max_date:                 " + str(max_date))
        chunked_delete(table, date_col, id_col,
                       group_by_col, max_date, chunk_size, session)

    def snapshot_before(**context):
        # Fail fast (visibly) if the recipient isn't configured — otherwise the
        # before/after email renders an empty `to` and is silently dropped.
        from airflow.models import Variable
        if not str(Variable.get("db_maintenance_alert_email", "")).strip():
            raise ValueError("Variable 'db_maintenance_alert_email' is not set. Set it "
                             "(comma-separated for multiple) so cleanup reports can be delivered.")
        context["ti"].xcom_push(key="region_env", value=_region_env())
        return _snapshot()   # -> XCom

    def build_report(**context):
        db = _fetch("SELECT current_database() AS d")[0]["d"]
        label = f"{_region_env()} — {db}"
        before = context["ti"].xcom_pull(task_ids="snapshot_before") or []
        after = _snapshot()
        deleted = sum(max(b["rows"] - a["rows"], 0)
                      for a in after for b in before if b["relname"] == a["relname"])
        note = f"Cleanup completed. Approx rows removed (incl. cascade): {deleted:,}"
        log.info(note)
        return _diff_html(before, after, label, f"<b>{note}</b>")

    # (table, date_col, id_col, group_by_col)
    # ONLY tables that do NOT cascade from dag_run are cleaned directly. The
    # children (task_instance, xcom, task_fail, rendered_task_instance_fields,
    # task_map, task_reschedule, ...) are removed automatically by ON DELETE
    # CASCADE when their dag_run is deleted — cleaning them here is redundant.
    tables = [
        ("dag_run", "start_date", "id", "dag_id"),
        ("job", "latest_heartbeat", "id", None),
        ("log", "dttm", "id", None),
        ("session", "expiry", "id", None),
        ("trigger", "created_date", "id", None),
    ]

    snapshot_before = rail.PythonOperator(
        task_id="snapshot_before", python_callable=snapshot_before,
        execution_timeout=timedelta(minutes=10), retries=1,
    )
    build_report = rail.PythonOperator(
        task_id="build_report", python_callable=build_report,
        execution_timeout=timedelta(minutes=10), retries=1,
    )
    email_success = rail.EmailOperator(
        task_id="email_success", to=ALERT_EMAIL,
        subject="[DB Cleanup][{{ ti.xcom_pull(task_ids='snapshot_before', key='region_env') or 'unknown' }}] SUCCESS - {{ ds }}",
        html_content="{{ ti.xcom_pull(task_ids='build_report') | safe }}",
    )
    email_failure = rail.EmailOperator(
        task_id="email_failure", trigger_rule="one_failed", to=ALERT_EMAIL,
        subject="[DB Cleanup][{{ ti.xcom_pull(task_ids='snapshot_before', key='region_env') or 'unknown' }}] FAILED - {{ ds }}",
        html_content=(
            "<h3>Airflow DB Cleanup FAILED</h3>"
            "<p>DAG: {{ dag.dag_id }}<br>Run: {{ run_id }}<br>Execution: {{ ts }}</p>"
            "<p>A task failed — check task logs:<br>"
            "{{ conf.get('webserver','base_url') }}/dags/{{ dag.dag_id }}/grid</p>"
        ),
    )

    # list comprehension so no lingering operator variable (repo lint:
    # test_task_vars_match_task_id) — comprehension vars don't leak in Py3.
    cleanup_tasks = [
        rail.PythonOperator(
            task_id=f'cleanup_{table}',
            python_callable=cleanup_table,
            op_args=[table, date_col, id_col, group_by_col],
            execution_timeout=timedelta(days=2),
            retries=3,
        )
        for table, date_col, id_col, group_by_col in tables
    ]

    # snapshot -> all cleanups -> report -> success email; failure email on any failure
    snapshot_before >> cleanup_tasks >> build_report >> email_success
    [snapshot_before] + cleanup_tasks + [build_report] >> email_failure
