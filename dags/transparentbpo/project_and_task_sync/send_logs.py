from pendulum import datetime, now
from datetime import datetime as py_datetime, timedelta
import rail
from airflow.models import Variable, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.session import NEW_SESSION, provide_session


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_send_logs_dag_id,
        description='Transparentbpo - Project & Task sync Send Logs',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 4, 10, tz=config.time_zone),
        schedule_interval=config.final_log_generation_dag_schedule_interval,
        max_active_runs=config.max_active_runs_send_logs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
        
        @provide_session
        def get_dagruns_to_process(session=NEW_SESSION):

            current_time = now(config.time_zone)
            lookup_timestamp_value = Variable.get(
                config.lookup_log_timestamp_var, default_var=None)

            query_end_date = py_datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
                current_time - timedelta(hours=config.lookup_log_timestamp_hours))

            Variable.set(config.lookup_log_timestamp_var,
                            current_time.isoformat())

            dag_runs_to_filter = (
                session.query(DagRun.id, DagRun.dag_id,
                                DagRun.state, DagRun.end_date)
                .select_from(DagRun)
                .filter(
                    DagRun.dag_id == config.process_logs_pregeneration_dag_id, DagRun.state.in_(
                        [DagRunState.SUCCESS]), (DagRun.end_date >= query_end_date))
                .group_by(DagRun.id, DagRun.dag_id, DagRun.state, DagRun.end_date)
                .all()
            )
            dag_runs = [item[0]
                        for item in dag_runs_to_filter] if dag_runs_to_filter else []

            return dag_runs

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process
        )
        
        get_timestamp_for_log_file = rail.PythonOperator(
            task_id='get_timestamp_for_log_file',
            python_callable=lambda: (now(config.time_zone) - timedelta(minutes = 60)).strftime(
                config.log_file_timestamp_format)
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_logs',
            no_task='no_pregeneration_log_dag_runs_to_process'
        )

        no_pregeneration_log_dag_runs_to_process = rail.EmptyOperator(
            task_id='no_pregeneration_log_dag_runs_to_process',
        )

        get_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='format_logs',
            flatten=True
        )

        compose_logs_collection = rail.CreateCollectionOperator(
            task_id='compose_logs_collection',
            source=lambda: rail.result('get_logs'),
            name='logs_collection'
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('compose_logs_collection', 'length') > 0 }}",
            yes_task='render_logs_csv',
            no_task='no_data_in_logs'
        )

        no_data_in_logs = rail.EmptyOperator(
            task_id='no_data_in_logs',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('compose_logs_collection') }}",
            header=['employeenumber', 'projectname(department)', 'tasklevel1(LaborLevel)',
                    'tasklevel2(Jobtitle)', 'status', 'details', 'jobid'],
            row=[
                "{{ item.employeenumber }}",
                "{{ item.projectname }}",
                "{{ item.tasklevel1 }}",
                "{{ item.tasklevel2 }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.ecid }}"
            ]
        )

        sftp_file_name = rail.PythonOperator(
            task_id='sftp_file_name',
            python_callable=lambda: config.log_filepath +
            "bamboohrprojecttasksynclog_" + rail.result('get_timestamp_for_log_file') + ".csv"
        )

        sftp_upload = rail.SFTPUploadFileOperator(
            task_id='sftp_upload',
            content="{{ result('render_logs_csv') }}",
            remote_filepath="{{ result('sftp_file_name') }}",
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name="{{ result('sftp_file_name') }}",
            expires_in_seconds=7*24*60*60,  # 7 days
        )
        
        get_failed_log_records = rail.QueryCollectionOperator(
            task_id='get_failed_log_records',
            query="""SELECT * FROM logs_collection WHERE status == 'Error'""",
        )

        if_failed_log_present = rail.IfOperator(
            task_id='if_failed_log_present',
            test="{{ result('get_failed_log_records', 'length') > 0 }}",
            yes_task='send_complete_mail_with_failed_records',
            no_task='send_complete_mail'
        )

        send_complete_mail_with_failed_records = rail.EmailOperator(
            task_id='send_complete_mail_with_failed_records',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Bamboohr Project and Task sync to Replicon completed with failed records - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/send_complete_mail_with_failed_records.html"
        )

        send_complete_mail = rail.EmailOperator(
            task_id='send_complete_mail',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Bamboohr Project and Task sync to Replicon completed successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/send_complete_mail.html"
        )
        
        get_log_dagruns_to_process >> get_timestamp_for_log_file >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            "No") >> no_pregeneration_log_dag_runs_to_process
        is_log_dagruns_present >> rail.Label("Yes") >> get_logs

        get_logs >> compose_logs_collection >> has_any_data

        has_any_data >> rail.Label("No") >> no_data_in_logs
        has_any_data >> rail.Label("Yes") >> render_logs_csv

        render_logs_csv >> sftp_file_name >> sftp_upload >>\
            generate_download_link >> get_failed_log_records >> if_failed_log_present 

        if_failed_log_present >> rail.Label(
                "Yes") >> send_complete_mail_with_failed_records

        if_failed_log_present >> rail.Label("No") >> send_complete_mail

    return dag


rail.for_each_instance(create_main_airflow_dag)
