"""
Replicon dag to send email alerts for tasks running for long duration
"""
from datetime import datetime, timedelta, timezone
import os
from urllib.parse import quote
import airflow
import rail
from sqlalchemy.sql import text
from airflow import settings
from airflow.models import DagBag, DagModel, DagRun, TaskInstance, Variable
from airflow.utils.state import TaskInstanceState
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.configuration import conf
from system.long_running_task_alerts import config

# In Airflow 2.10+, the `_try_number` SQLAlchemy column was renamed to `try_number`
# (the old name became a deprecated @property). Pick the correct attribute so the
# same DAG works on both 2.7 and 2.11 environments during the rollout.
_AIRFLOW_VERSION = tuple(int(x) for x in airflow.__version__.split('.')[:2])
_TRY_NUMBER_COLUMN = (
    TaskInstance.try_number if _AIRFLOW_VERSION >= (2, 10) else TaskInstance._try_number
)

with airflow.DAG(
    dag_id="system_long_running_task_alerts",
    schedule=timedelta(minutes=30),
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

    def days_hours_minutes(time_delta):
        seconds = time_delta.total_seconds()
        return f'{int(seconds//3600)}h:{int(time_delta.seconds%3600//60)}m:{int(seconds%60)}s'

    # pylint: disable=protected-access
    @provide_session
    def get_long_running_tasks_by_count(session=NEW_SESSION):
        current_time = datetime.now(timezone.utc)
        base_url = conf.get('webserver', 'BASE_URL')
        long_running_check_minutes = Variable.get(
            'system_long_running_task_alerts_threshold_minutes', default_var='30')

        threshold = int(long_running_check_minutes)
        timestamp = current_time - \
            timedelta(minutes=threshold)
        queried_tasks = (
            session.query(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state, TaskInstance.run_id,
                          TaskInstance.start_date, DagRun.execution_date, _TRY_NUMBER_COLUMN, TaskInstance.job_id)
            .join(DagModel, (DagModel.dag_id == TaskInstance.dag_id) & DagModel.is_active)
            .join(DagRun, (DagRun.dag_id == TaskInstance.dag_id) & (DagRun.run_id == TaskInstance.run_id))
            .filter(TaskInstance.state.in_([TaskInstanceState.RUNNING, TaskInstanceState.DEFERRED,
                                            TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED]),
                    (TaskInstance.start_date < timestamp),
                    (TaskInstance.trigger_id.is_(None)))
            .all()
        )
        queried_tasks_list = []
        if queried_tasks:
            for dag_id, task_id, state, run_id, start_date, execution_date, try_number, job_id in queried_tasks:
                latest_heartbeat = get_latest_heartbeat(
                    session, dag_id,  job_id)
                queried_tasks_list.append(
                    {
                        'dag_id': dag_id,
                        'task_id': task_id,
                        'state': state,
                        'run_id': run_id,
                        'try_number': try_number,
                        'start_date': start_date.isoformat(),
                        'duration': days_hours_minutes(current_time - start_date),
                        'execution_date': execution_date.isoformat(),
                        'latest_heartbeat': latest_heartbeat.isoformat() if latest_heartbeat else None,
                        'link': f'{base_url}/log?dag_id={dag_id}&task_id={task_id}&execution_date={quote(execution_date.isoformat())}'
                    })

        return {'task_count': len(queried_tasks_list), 'threshold': threshold, 'dags': queried_tasks_list, 'region': os.environ.get('REGION', 'unknown'),
                'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown'), 'threshold_timestamp': timestamp.isoformat(), }

    # pylint: disable=too-many-arguments
    def get_latest_heartbeat(session, dag_id, job_id):
        latest_heartbeat = list(session.execute(text(
            f"SELECT latest_heartbeat FROM job WHERE id={job_id or -1} AND dag_id='{dag_id}'")))
        return latest_heartbeat[0][0] if latest_heartbeat else None

    get_long_running_task_dags = rail.PythonOperator(
        task_id="get_long_running_task_dags",
        priority_weight=10,
        python_callable=get_long_running_tasks_by_count
    )

    should_send_alert = rail.IfOperator(
        task_id="should_send_alert",
        test=lambda: rail.result('get_long_running_task_dags') and
        rail.result('get_long_running_task_dags')['dags'] and list(filter(lambda x: x['latest_heartbeat'] is None or
                                                                          datetime.fromisoformat(x['latest_heartbeat']) <
                                                                          datetime.fromisoformat(rail.result('get_long_running_task_dags')[
                                                                                                 'threshold_timestamp']),
                                                                          rail.result('get_long_running_task_dags')['dags'])),
        yes_task='render_csv',
        no_task='delete_this_dagrun'
    )

    get_stalled_tasks = rail.PythonOperator(
        task_id="get_stalled_tasks",
        python_callable=lambda: list(filter(lambda x: x['latest_heartbeat'] is None or datetime.fromisoformat(
            x['latest_heartbeat']) < datetime.fromisoformat(rail.result('get_long_running_task_dags')['threshold_timestamp']),
            rail.result('get_long_running_task_dags')['dags']))
    )

    has_stalled_tasks_for_retry = rail.IfOperator(
        task_id="has_stalled_tasks_for_retry",
        test=lambda: rail.result('get_stalled_tasks') and Variable.get(
            'system_long_running_task_alerts_retry_task', default_var='false').lower() == 'true',
        yes_task='fail_stalled_tasks',
    )

    def do_fail_stalled_tasks():
        session = settings.Session()
        stalled_tasks = rail.result('get_stalled_tasks')
        dag_bag = DagBag(read_dags_from_db=True)
        for task in stalled_tasks:
            print(f'Started Marking task for {task["link"]} as failed')
            task_dag = dag_bag.get_dag(task['dag_id'], session=session)
            if not task_dag:
                print(f"DAG {task['dag_id']} not found")
                continue
            task_dag.set_task_instance_state(task_id=task['task_id'],
                                             run_id=task['run_id'], execution_date=task['execution_date'],
                                             state=TaskInstanceState.FAILED, downstream=True, session=session)
            print(f"Done marking the task {task['link']} as failed")
        session.commit()
        session.close()

    fail_stalled_tasks = rail.PythonOperator(
        task_id="fail_stalled_tasks",
        python_callable=do_fail_stalled_tasks
    )

    def do_clear_stalled_tasks():
        session = settings.Session()
        stalled_tasks = rail.result('get_stalled_tasks')
        dag_bag = DagBag(read_dags_from_db=True)
        for task in stalled_tasks:
            print(f'Started retriggerring/clearing task for {task["link"]}')
            task_dag = dag_bag.get_dag(task['dag_id'], session=session)
            if not task_dag:
                print(f"DAG {task['dag_id']} not found")
                continue
            print(f"Clearing the task {task['link']}")
            tasks = task_dag.clear(
                start_date=datetime.fromisoformat(task['execution_date']),
                end_date=datetime.fromisoformat(task['execution_date']),
                task_ids=[task['task_id']],
                session=session
            )
            print('Cleared tasks count ', tasks)
        session.commit()
        session.close()

    clear_stalled_tasks = rail.PythonOperator(
        task_id="clear_stalled_tasks",
        python_callable=do_clear_stalled_tasks
    )

    send_retry_email_alert = rail.EmailOperator(
        task_id='send_retry_email_alert',
        to='{{ var.value.dagrun_failure_alert_email }}',
        # pylint: disable=line-too-long
        subject="{{ result('get_long_running_task_dags').environment }} | {{ result('get_long_running_task_dags').region }} | {{ result('get_stalled_tasks') | length}} stalled tasks are retriggered - {{ current_time_in_specified_tz() }}",
        html_content='email_template_retry.html'
    )

    render_csv = rail.WriteCSVFileOperator(
        task_id='render_csv',
        source=lambda: rail.result('get_long_running_task_dags')['dags'],
        header=[
            'DAG ID',
            'Task ID',
            'State',
            'DAG Run ID',
            'Start Date (ISO Format)',
            'Duration',
            'DAG Run Link',
            'Latest heartbeat'
        ],
        row=[
            '{{ item.dag_id }}',
            '{{ item.task_id }}',
            '{{ item.state }}',
            '{{ item.run_id }}',
            '{{ item.start_date }}',
            '{{ item.duration }}',
            '{{ item.link }}',
            '{{ item.latest_heartbeat }}'
        ]
    )

    generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
        task_id='generate_download_link',
        artifact_name="{{ result('render_csv') }}",
        output_file_name='task_long_run_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        expires_in_seconds=4*24*60*60
    )

    send_email_alert = rail.EmailOperator(
        task_id='send_email_alert',
        to='{{ var.value.dagrun_failure_alert_email }}',
        # pylint: disable=line-too-long
        subject="{{ result('get_long_running_task_dags').environment }} | {{ result('get_long_running_task_dags').region }} | {{ result('get_long_running_task_dags').task_count }} long running tasks found - {{ current_time_in_specified_tz() }}",
        html_content='email_template.html'
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        priority_weight=10,
        task_id='delete_this_dagrun')

    get_long_running_task_dags >> should_send_alert
    get_long_running_task_dags >> get_stalled_tasks >> has_stalled_tasks_for_retry
    has_stalled_tasks_for_retry >> rail.Label(
        'yes') >> fail_stalled_tasks >> clear_stalled_tasks >> send_retry_email_alert

    should_send_alert >> rail.Label(
        "Yes") >> render_csv >> generate_download_link >> send_email_alert

    should_send_alert >> rail.Label(
        "No") >> delete_this_dagrun
