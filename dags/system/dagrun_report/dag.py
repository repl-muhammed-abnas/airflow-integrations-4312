"""
Replicon dag to create dagrun report
"""
from datetime import datetime, timedelta
import airflow
import os
import rail
from airflow.utils.session import NEW_SESSION, provide_session
from system.dagrun_report import config

with airflow.DAG(
    dag_id="system_dagrun_report",
    start_date=datetime(2022, 1, 1),
    schedule=timedelta(days=30),
    catchup=False,
    tags=['system'],
    is_paused_upon_creation=True,
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
        'replicon_conn_id': config.replicon_conn_id
    },
    default_view="graph"
) as dag:

    batch_task = rail.BatchTaskRunOperator(
        task_id='batch_task',
        start_task='dagrun_query_result',
        end_task='finish',
        execution_timeout=timedelta(days=14)
    )

    @provide_session
    def get_dagrun_query_result(session=NEW_SESSION):
        query = session.execute(
            f'''
                SELECT
                    d.dag_id,
                    d.max_active_runs,
                    COUNT(dr.dag_id)
                FROM
                    dag d
                LEFT JOIN
                    dag_run dr ON d.dag_id = dr.dag_id
                WHERE
                    d.is_active = true
                GROUP BY
                    d.dag_id;
            ''')

        records = list(map(lambda x: {
            'dag_id': f'{str(x[0])}',
            'max_active_runs': f'{str(x[1])}',
            'dag_run_count': f'{str(x[2])}'
        }, query))

        total_dag_run_count = sum(int(x['dag_run_count']) for x in records)
        total_max_active_runs = sum(int(x['max_active_runs']) for x in records)
        return {
            'dags': records,
            'active_dags_count': str(len(records)),
            'total_dag_run_count': total_dag_run_count,
            'total_max_active_runs': total_max_active_runs,
            'region': os.environ.get('REGION', 'unknown'),
            'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')
        }

    dagrun_query_result = rail.PythonOperator(
        task_id='dagrun_query_result',
        python_callable=get_dagrun_query_result
    )

    render_csv = rail.WriteCSVFileOperator(
        task_id='render_csv',
        source=lambda: rail.result('dagrun_query_result')['dags'],
        header=[
            'DAG ID',
            'Max Active Runs',
            'Dag Run Count'
        ],
        row=[
            '{{ item.dag_id }}',
            '{{ item.max_active_runs }}',
            '{{ item.dag_run_count }}'
        ],
        footer=[
            'Total',
            '{{ result("dagrun_query_result").total_max_active_runs}}',
            '{{ result("dagrun_query_result").total_dag_run_count }}'
        ]
    )

    generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
        task_id='generate_download_link',
        artifact_name="{{ result('render_csv') }}",
        output_file_name='dag_run_report_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        expires_in_seconds=4*24*60*60
    )

    send_email_alert = rail.EmailOperator(
        task_id='send_email_alert',
        to='{{ var.value.dagrun_failure_alert_email }}',
        subject="{{ result('dagrun_query_result').environment }} \
                    | {{ result('dagrun_query_result').region }} \
                    | {{ result('dagrun_query_result').total_max_active_runs }} \
                    Total Max active runs - {{ current_time_in_specified_tz() }}",
        html_content='email_template.html'
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun'
    )

    finish = rail.EmptyOperator(task_id='finish')

    (
        batch_task
        >> dagrun_query_result
        >> render_csv
        >> generate_download_link
        >> send_email_alert
        >> delete_this_dagrun
        >> finish
    )
    batch_task >> finish
