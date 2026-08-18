from datetime import datetime as dt, timedelta, timezone
from functools import lru_cache
from pendulum import datetime
import rail
from airflow.models import Variable, DagRun


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= 'crl_project_import_adhoc_dag',
        description= "adhoc to download logs",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = 1,
        default_args= {
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):

            current_time = dt.now(timezone.utc)
            lookup_timestamp_value = Variable.get(
                lookup_log_timestamp_var, default_var=None)

            query_execution_start_date = dt.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
                current_time - timedelta(hours=lookup_log_timestamp_hours))

            dag_runs = []
            execution_dates = []
            for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
                execution_dates.append(run.execution_date)
                dag_runs.append(run.id)
            if execution_dates:
                max_execution_date = max(execution_dates)
                Variable.set(lookup_log_timestamp_var,
                            (max_execution_date + timedelta(seconds=1)).isoformat())
            return dag_runs

        get_dag_runs_list = rail.PythonOperator(
            task_id='get_dag_runs_list',
            python_callable=get_dagruns_to_process,
            op_args=[config.look_up_stamp_var,config.lookup_stamp_hours,config.dag_id]
        )

        get_collection_artifacts = rail.GatherResultsFromDagRunsOperator(
            task_id='get_collection_artifacts',
            execution_timeout=timedelta(days=14),
            dag_runs="{{ result('get_dag_runs_list') }}",
            dagrun_task_id='create_collection_input_data',
            flatten=True
        )


        @lru_cache(maxsize=128)
        def load_all_data():
            items = rail.result("get_collection_artifacts")
            load_data= [rail.load_all_records(item) for item in items]

            final_data =  [
                {'projectname': item['projectname'], 'projectcode': item['projectcode'],
                 'clientname': item['clientname'], 'clientcode': item['clientcode']}
                for sublist in load_data
                for item in sublist
            ]

            return final_data

        load_artifacts_data = rail.PythonOperator(
            task_id = 'load_artifacts_data',
            python_callable= load_all_data,
            execution_timeout=timedelta(days=14)
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{ result('load_artifacts_data') | to_json}}",
            header=[
                'projectname',
                'projectcode',
                'clientname',
                'clientcode'
            ],
            row=[
                "{{item.projectname}}",
                "{{item.projectcode}}",
                "{{item.clientname}}",
                "{{item.clientcode}}"
            ],
        )

        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id = 'upload_to_sftp',
            content= '{{ result("create_csv_log") }}',
            remote_filepath= '/crl/project/Collections/project_import_collections.csv'
        )

        get_dag_runs_list >> get_collection_artifacts >> load_artifacts_data >> create_csv_log >> upload_to_sftp

    return dag

rail.for_each_instance(create_main_dag)
