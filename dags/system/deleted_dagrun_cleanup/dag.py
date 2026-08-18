"""
A maintenance workflow that runs periodically to actually delete any dagruns which have indicated that they want to
be deleted. The only reason for this is to keep the dagrun history cleaner and less cluttered with empty dagruns.
"""
from datetime import datetime, timedelta
import airflow
import rail


with airflow.DAG(
    dag_id='system_deleted_dagrun_cleanup',
    schedule=timedelta(hours=2),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['system_maintenance'],
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
) as dag:
    execute_pending_dagrun_deletions = rail.ExecutePendingDagRunDeletionsOperator(
        task_id='execute_pending_dagrun_deletions')
    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun')
    execute_pending_dagrun_deletions >> delete_this_dagrun
