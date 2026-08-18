"""
Replicon dag to send email alerts for DAGs running or queued for long duration
"""
from datetime import datetime, timedelta, timezone
from collections import Counter
import os
from urllib.parse import urlencode
import airflow
import rail
from airflow.models import DagModel, DagRun, Variable
from airflow.utils.state import DagRunState
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.configuration import conf
from system.queued_dagrun_alerts import config

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/queued_dagrun_alerts/config.py

with airflow.DAG(
    dag_id="queued_dagrun_alerts",
    schedule=timedelta(hours=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['system_maintenance'],
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
        'replicon_conn_id': config.replicon_conn_id
    },
    default_view="graph",
    max_active_runs=1
) as dag:

    @provide_session
    def get_running_queued_dags_by_count(session=NEW_SESSION):
        current_time = datetime.now(timezone.utc)
        base_url = conf.get('webserver', 'BASE_URL')
        pending_dagrun_check_hours = Variable.get(
            'pending_dag_run_check_hours', default_var=2)

        threshold = int(pending_dagrun_check_hours) if isinstance(
            pending_dagrun_check_hours, str) else pending_dagrun_check_hours

        timestamp = current_time - \
            timedelta(hours=threshold)
        queried_dags = (
            session.query(DagRun.dag_id, DagRun.state, DagRun.run_id,
                          DagRun.queued_at, DagRun.execution_date)
            .select_from(DagRun)
            .filter(DagRun.state.in_([DagRunState.QUEUED, DagRunState.RUNNING]), (DagRun.queued_at < timestamp))
            .outerjoin(DagModel, (DagRun.dag_id == DagModel.dag_id) & DagModel.is_active)
            .group_by(DagRun.dag_id, DagRun.state, DagRun.run_id, DagRun.queued_at, DagRun.execution_date)
            .all()
        )

        queried_dags_list = []
        if queried_dags:
            def get_dag_run_link(base_url, dag_id, run_id, execution_date):
                from airflow.version import version
                from packaging.version import Version
                if Version(version) >= Version('2.7.3'):
                    url_params = {
                        "run_id": run_id,
                        "execution_date": execution_date.isoformat(),
                        "base_date": execution_date.isoformat(),
                        "tab": "graph",
                        "dag_run_id": run_id
                    }
                    return f'{base_url}/dags/{dag_id}/grid?{urlencode(url_params)}'
                url_params = {
                    "dag_id": dag_id,
                    "execution_date": execution_date.isoformat()
                }
                return f'{base_url}/graph?{urlencode(url_params)}'
            for dag_id, state, run_id, queued_at, execution_date in queried_dags:
                queried_dags_list.append(
                    {
                        'dag_id': dag_id,
                        'state': state,
                        'run_id': run_id,
                        'queued_at': queued_at.isoformat(),
                        'link': get_dag_run_link(base_url, dag_id, run_id, execution_date)
                    })

        return {'dag_count': len(queried_dags_list), 'threshold': threshold, 'dags': queried_dags_list, 'region': os.environ.get('REGION', 'unknown'),
                'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown'), }

    get_long_queued_running_dags = rail.PythonOperator(
        task_id="get_long_queued_running_dags",
        priority_weight=10,
        python_callable=get_running_queued_dags_by_count
    )

    def get_count_of_dag_runs():
        dag_runs_count = Counter(item['dag_id'] for item in rail.result(
            'get_long_queued_running_dags')['dags'])
        final_list = []

        for item in dag_runs_count:
            final_list.append({
                'dag_id': item,
                'count': dag_runs_count.get(item)
            })

        return sorted(final_list, key=lambda x: x['count'], reverse=True)

    get_count_of_dagruns = rail.PythonOperator(
        task_id="get_count_of_dagruns",
        priority_weight=10,
        python_callable=get_count_of_dag_runs
    )

    should_send_alert = rail.IfOperator(
        task_id="should_send_alert",
        test=lambda: rail.result('get_long_queued_running_dags') and
        rail.result('get_long_queued_running_dags')[
                'dag_count'] > 0,
        yes_task='render_csv',
        no_task='delete_this_dagrun'
    )

    render_csv = rail.WriteCSVFileOperator(
        task_id='render_csv',
        source=lambda: rail.result('get_long_queued_running_dags')['dags'],
        header=[
            'DAG ID',
            'State',
            'DAG Run ID',
            'Queued at (ISO Format)',
            'DAG Run Link'
        ],
        row=[
            '{{ item.dag_id }}',
            '{{ item.state }}',
            '{{ item.run_id }}',
            '{{ item.queued_at }}',
            '{{ item.link }}'
        ]
    )

    generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
        task_id='generate_download_link',
        artifact_name="{{ result('render_csv') }}",
        output_file_name='dag_run_queued_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        expires_in_seconds=4*24*60*60
    )

    send_email_alert = rail.EmailOperator(
        task_id='send_email_alert',
        to='{{ var.value.dagrun_failure_alert_email }}',
        # pylint: disable=line-too-long
        subject="{{ result('get_long_queued_running_dags').environment }} | {{ result('get_long_queued_running_dags').region }} | {{ result('get_long_queued_running_dags').dag_count }} long running or queued dag runs found - {{ current_time_in_specified_tz() }}",
        html_content='email_template.html'
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        priority_weight=10,
        task_id='delete_this_dagrun')

    get_long_queued_running_dags >> get_count_of_dagruns >> should_send_alert

    should_send_alert >> rail.Label(
        "Yes") >> render_csv >> generate_download_link >> send_email_alert

    should_send_alert >> rail.Label(
        "No") >> delete_this_dagrun
