"""
### System DAG for getting the scheduled DAG runs
#### This DAG retrieves the scheduled DAG runs in the Airflow environment and generates a CSV file with the details.
#### The CSV file is then used to send an email alert with a download link.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG as af_dag
from airflow.exceptions import AirflowFailException
from airflow.models import DagBag, DagModel
from airflow.utils.session import NEW_SESSION, provide_session

import pendulum
import rail

from system.get_scheduled_dagruns import config


with af_dag(
    dag_id='system_get_scheduled_dagruns',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['system'],
    is_paused_upon_creation=True,
    doc_md=__doc__,
    max_active_runs=1,
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'replicon_conn_id': config.replicon_conn_id
    },
) as dag:

    @provide_session
    def get_scheduled_dags(session=NEW_SESSION):

        non_scheduled_dag_description = "Never, external triggers only"
        TIME_FORMAT = "YYYY-MM-DD HH:mm:ss"
        FILE_SENSOR_DAG_INTERVAL = 30

        dag_run_conf = rail.get_current_context()['dag_run'].conf
        earliest = pendulum.from_format(
            dag_run_conf['start_datetime'], TIME_FORMAT) if dag_run_conf.get('start_datetime') else None
        latest = pendulum.from_format(
            dag_run_conf['end_datetime'], TIME_FORMAT) if dag_run_conf.get('end_datetime') else None
        if not latest or ((latest - earliest).seconds > config.max_days_difference * 24 * 60 * 60):
            raise AirflowFailException(
                'Incorrect Datetime Input or Datetime Input Difference is too large')

        schedule_interval_dags = (
            session.query(DagModel.dag_id,
                          DagModel.schedule_interval,
                          DagModel.timetable_description)
            .filter(~DagModel.is_paused,  # Ignore paused dags
                    ~DagModel.dag_id.startswith('system'),  # Ignore system dags
                    ~DagModel.dag_id.startswith('standard'),  # Ignore standard oob connector dags
                    DagModel.timetable_description != non_scheduled_dag_description,
                    DagModel.is_active)
            .all()
        )

        if not schedule_interval_dags:
            return []

        dag_ids = [dag.dag_id for dag in schedule_interval_dags]
        try:
            dag_bag = DagBag(dag_ids=dag_ids, read_dags_from_db=True)
        except TypeError:
            # Fallback for older Airflow: load all, then filter
            dag_bag = DagBag(read_dags_from_db=True)

        scheduled_dagrun_info = []
        total_count = 0
        for scheduled_dag in schedule_interval_dags:
            serialized_dag = dag_bag.get_dag(scheduled_dag.dag_id)
            if not serialized_dag:
                continue  # Skip missing DAGs
            if not isinstance(serialized_dag.schedule_interval, str) and \
                    serialized_dag.schedule_interval.seconds == FILE_SENSOR_DAG_INTERVAL:
                continue  # Skip file sensor DAGs

            dag_run_info = [x for x in serialized_dag.iter_dagrun_infos_between(
                earliest, latest) if x.data_interval and x.data_interval.end <= latest and x.data_interval.end >= earliest]
            info_count = len(dag_run_info)
            if info_count == 0:
                continue
            total_count += info_count
            scheduled_dagrun_info.extend([
                {
                    'dag_id': scheduled_dag.dag_id,
                    'schedule_interval': str(scheduled_dag.schedule_interval),
                    'timetable_description': scheduled_dag.timetable_description if scheduled_dag.timetable_description else 'Not Available',
                    'scheduled_run_in_utc': x.data_interval.end.format(TIME_FORMAT)
                } for x in dag_run_info])

        rail.set_result(
            key="region-environment", val=f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'dev')}")
        rail.set_result(key="count", val=total_count)
        return scheduled_dagrun_info

    scheduled_dags = rail.PythonOperator(
        task_id="scheduled_dags",
        python_callable=get_scheduled_dags,
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes)
    )

    is_scheduled_dags_present = rail.IfOperator(
        task_id="is_scheduled_dags_present",
        test="{{ result('scheduled_dags', 'count') | sn | is_truthy and \
            result('scheduled_dags', 'count') > 0 }}",
        yes_task='write_csv',
        no_task='delete_this_dagrun'
    )

    write_csv = rail.WriteCSVFileOperator(
        task_id='write_csv',
        source=lambda: rail.result('scheduled_dags'),
        header=[
            'DAG ID',
            'Schedule Interval',
            'Time table Description',
            'Scheduled Run in UTC'
        ],
        row=[
            '{{ item.dag_id }}',
            '{{ item.schedule_interval }}',
            '{{ item.timetable_description }}',
            '{{ item.scheduled_run_in_utc }}'
        ]
    )

    generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
        task_id='generate_download_link',
        artifact_name="{{ result('write_csv') }}",
        output_file_name="dag_run_scheduled_{{ result('scheduled_dags', 'region-environment') }}.csv",
        expires_in_seconds=4*24*60*60
    )

    send_email_alert = rail.EmailOperator(
        task_id='send_email_alert',
        to=config.alert_email,
        # pylint: disable=line-too-long
        subject="{{ result('scheduled_dags', 'region-environment') }} | {{ result('scheduled_dags', 'count') }} Dag runs to be scheduled from {{ dag_run.conf.start_datetime | default('N/A') }} to {{ dag_run.conf.end_datetime | default('N/A') }}",
        html_content='email_template.html'
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        priority_weight=10,
        task_id='delete_this_dagrun')

    scheduled_dags >> is_scheduled_dags_present

    is_scheduled_dags_present >> rail.Label(
        "Yes") >> write_csv >> generate_download_link >> send_email_alert

    is_scheduled_dags_present >> rail.Label(
        "No") >> delete_this_dagrun
