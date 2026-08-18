from datetime import timedelta
from pendulum import now
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_report_export_ts_child_{config.instance}',
        description=f'Pwcfr_report_export_ts_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='start'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='start',
            end_task='finish_job',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        start = rail.EmptyOperator(
            task_id='start'
        )

        generate_report = rail.run_report2(
            group_id='generate_report_data',
            report_params={
                "reportParameters":  [
                    {
                        "filterValues": [{
                            "reportFilterUri": "{{dag_run.conf.filteruri}}",
                            "value": "null"
                        },
                            {
                            "reportFilterUri": "{{dag_run.conf.filteruri}}",
                            "value": "{{dag_run.conf.startdate}}"
                        },
                            {
                            "reportFilterUri": "{{dag_run.conf.filteruri}}",
                            "value": "{{dag_run.conf.enddate}}"
                        }],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{dag_run.conf.reporturi}}"
                    }
                ]
            },
            target='artifact',
        )

        if_payload_contains_error = rail.IfOperator(
            task_id='if_payload_has_error',
            test="{{ (result('generate_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="stop_job_with_error",
            no_task="if_payload_has_data",
        )

        stop_job_with_error = rail.FailOperator(
            task_id='stop_job_with_error',
            message="{{(result('generate_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}"
        )

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='{{not (result("generate_report_data.get_report_result")| load_json_artifact).reportGenerationResults[0].payload | matches("No Data")}}',
            yes_task="upload_file_to_sftp",
            no_task="stop_job"
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp',
            content="{{ (result('generate_report_data.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}",
            remote_filepath="{{dag_run.conf.filepath}}" +
            '/TS_replicon' + "{{dag_run.conf.month}}" +
            (now(tz='PST8PDT')).strftime("%d%m%Y") + '.csv'
        )

        stop_job = rail.EmptyOperator(
            task_id='stop_job',
        )

        finish_job = rail.EmptyOperator(
            task_id='finish_job'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish_job
        can_run_batch_task >> rail.Label('No') >> start >> generate_report
        generate_report >> if_payload_contains_error
        if_payload_contains_error >> rail.Label('Yes') >> stop_job_with_error
        if_payload_contains_error >> rail.Label('No') >> if_payload_has_data
        if_payload_has_data >> rail.Label(
            'Yes') >> upload_file_to_sftp >> finish_job >> log_to_sumo
        if_payload_has_data >> rail.Label('No') >> stop_job >> finish_job

        return dag


rail.for_each_instance(create_dag)
