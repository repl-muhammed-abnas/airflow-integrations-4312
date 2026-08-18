import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from datetime import datetime, timedelta
import os
import json
import airflow
from airflow.models import Variable
from airflow.utils.session import NEW_SESSION, provide_session
from google.oauth2 import service_account
from google.cloud import storage
import rail


with airflow.DAG(
    dag_id='system_task_usage_stats',
    schedule='0 0 * * *',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['system_maintenance'],
    is_paused_upon_creation=False,
    doc_md=__doc__,
    max_active_runs=1,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
) as dag:

    def do_get_last_run_date():
        last_run_date_var_name = 'system_task_usage_stats_last_run_date'
        current_time = datetime.utcnow() - timedelta(seconds=2)
        lookup_timestamp_value = Variable.get(last_run_date_var_name, default_var=(
            datetime.utcnow() - timedelta(days=60)).isoformat())
        last_run_date = (datetime.fromisoformat(
            lookup_timestamp_value) if lookup_timestamp_value else current_time).isoformat()
        Variable.set(last_run_date_var_name,
                     current_time.isoformat())
        return last_run_date

    get_last_run_date = rail.PythonOperator(
        task_id='get_last_run_date',
        python_callable=do_get_last_run_date
    )

    @provide_session
    def do_get_task_usage_from_db(session=NEW_SESSION):
        from sqlalchemy import text
        last_run_date = rail.result('get_last_run_date')
        region = os.environ.get('REGION', 'unknown')
        environment = os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')
        
        # Query only distinct dag_id metadata (small result set)
        dag_metadata = {}
        metadata_query = text('''
        SELECT DISTINCT dag.dag_id, dag.owners, dag.fileloc
        FROM dag
        INNER JOIN task_instance AS ti ON ti.dag_id = dag.dag_id
        WHERE ti.end_date >= :last_run_date
        ''')
        metadata_result = session.execute(metadata_query, {'last_run_date': last_run_date})
        for row in metadata_result:
            dag_metadata[row[0]] = {'owners': row[1], 'fileloc': row[2]}
        metadata_result.close()
        
        # Aggregate in SQL by task_date + dag_id only (no conf) with streaming
        query = text('''
        SELECT Date(ti.start_date) AS task_date,
            ti.dag_id,
            Count(*) AS task_count,
            Round(Sum(Extract(epoch FROM (ti.end_date - ti.start_date) / 60))::NUMERIC, 4) AS task_duration_min
        FROM task_instance AS ti
        WHERE ti.start_date IS NOT NULL 
            AND ti.end_date IS NOT NULL
            AND ti.state <> 'skipped'
            AND ti.end_date >= :last_run_date
        GROUP BY task_date, ti.dag_id
        ''')
        
        import io
        import csv
        output = io.StringIO()
        fields = ['task_date', 'region', 'enviroment', 'companykey',
                  'dag_file_loc', 'dag_id', 'task_count', 'task_duration_min']
        writer = csv.DictWriter(output, fieldnames=fields, delimiter=',')
        writer.writeheader()
        
        # Stream results with execution_options to avoid loading all rows into memory
        result = session.connection().execution_options(stream_results=True).execute(
            query, {'last_run_date': last_run_date}
        )
        
        batch = []
        batch_size = 1000
        for row in result:
            task_date, dag_id, task_count, task_duration_min = row
            metadata = dag_metadata.get(dag_id, {'owners': 'unknown', 'fileloc': ''})
            batch.append({
                'task_date': str(task_date),
                'region': region,
                'enviroment': environment,
                'companykey': metadata['owners'],
                'dag_file_loc': metadata['fileloc'],
                'dag_id': dag_id,
                'task_count': str(task_count),
                'task_duration_min': str(task_duration_min) if task_duration_min else '0'
            })
            
            if len(batch) >= batch_size:
                writer.writerows(batch)
                batch = []
        
        if batch:
            writer.writerows(batch)
        
        result.close()
        return output.getvalue()
    get_task_usage_from_db_in_csv = rail.PythonOperator(
        task_id='get_task_usage_from_db_in_csv',
        python_callable=do_get_task_usage_from_db
    )

    def do_upload_stats_to_datawarehouse():
        bucket_name = 'product-integration-logs-airflow'
        destination_blob_name = f"prod/airflow_task_usage_{datetime.utcnow().isoformat()}_{os.environ.get('REGION', 'unknown')}_{os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')}.csv"
        credentials_var_value = json.loads(Variable.get(
            'system_task_usage_stats_dw_secret_credentials'))
        credentials = service_account.Credentials.from_service_account_info(
            credentials_var_value)
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(rail.result('get_task_usage_from_db_in_csv'))

    upload_stats_to_datawarehouse = rail.PythonOperator(
        task_id='upload_stats_to_datawarehouse',
        python_callable=do_upload_stats_to_datawarehouse
    )


    @provide_session
    def do_get_active_dags_from_db(session=NEW_SESSION):
        from sqlalchemy import text
        region = os.environ.get('REGION', 'unknown')
        environment = os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')
        
        query = text('''
        SELECT 
            dag.owners AS companykey,
            dag.fileloc AS dag_file_loc,
            dag.dag_id,
            dag.is_active,
            dag.is_paused
        FROM dag
        WHERE dag.is_active = true AND dag.is_paused = false
        ''')
        
        import io
        import csv
        output = io.StringIO()
        fields = ['region', 'enviroment', 'companykey',
                  'dag_file_loc', 'dag_id', 'is_active', 'is_paused']
        writer = csv.DictWriter(output, fieldnames=fields, delimiter=',')
        writer.writeheader()
        
        result = session.execute(query)
        for row in result:
            writer.writerow({
                'region': region,
                'enviroment': environment,
                'companykey': str(row[0]),
                'dag_file_loc': str(row[1]),
                'dag_id': str(row[2]),
                'is_active': str(row[3]),
                'is_paused': str(row[4])
            })
        result.close()
        
        return output.getvalue()
    get_active_dags_from_db_in_csv = rail.PythonOperator(
        task_id='get_active_dags_from_db_in_csv',
        python_callable=do_get_active_dags_from_db
    )

    def do_upload_active_dags_to_datawarehouse():
        bucket_name = 'product-integration-logs-airflow'
        destination_blob_name = f"prod/airflow_active_dags_{datetime.utcnow().isoformat()}_{os.environ.get('REGION', 'unknown')}_{os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')}.csv"
        credentials_var_value = json.loads(Variable.get(
            'system_task_usage_stats_dw_secret_credentials'))
        credentials = service_account.Credentials.from_service_account_info(
            credentials_var_value)
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(rail.result('get_active_dags_from_db_in_csv'))

    upload_active_dags_to_datawarehouse = rail.PythonOperator(
        task_id='upload_active_dags_to_datawarehouse',
        python_callable=do_upload_active_dags_to_datawarehouse
    )

    get_last_run_date >> get_task_usage_from_db_in_csv >> upload_stats_to_datawarehouse >> get_active_dags_from_db_in_csv >> upload_active_dags_to_datawarehouse