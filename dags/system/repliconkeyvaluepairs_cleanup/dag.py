from datetime import datetime, timedelta, timezone
import airflow
import rail
from airflow.utils.session import NEW_SESSION, provide_session

with airflow.DAG(
    dag_id='system_repliconkeyvaluepairs_cleanup',
    schedule=timedelta(minutes=2),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['system_maintenance'],
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

    batch_task = rail.BatchTaskRunOperator(
        task_id='batch_task',
        start_task='execute_repliconkeyvaluepairs_cleanup',
        end_task='finish',
        execution_timeout=timedelta(minutes=2)
    )

    @provide_session
    def do_cleanup_session(session=NEW_SESSION):
        session.execute('DELETE FROM repliconkeyvaluepairs WHERE expiry < :now', {
                        'now': datetime.now(timezone.utc)})
        session.commit()
        # Run ANALYZE after DELETE
        session.execute('ANALYZE repliconkeyvaluepairs')
        
    execute_repliconkeyvaluepairs_cleanup = rail.PythonOperator(
        task_id='execute_repliconkeyvaluepairs_cleanup',
        python_callable=do_cleanup_session
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun')
    
    finish = rail.EmptyOperator(
        task_id='finish',
    )

    batch_task >> execute_repliconkeyvaluepairs_cleanup >> delete_this_dagrun >> finish
    batch_task >> finish
