from pendulum import datetime
from kmc.punchtimesheetdetailsreportextract.utils import custom_methods
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'kmc_punchtimesheetdetailsreportextract_{config.instance}',
        description=f'punchtimesheetdetailsreportextract {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2023, 1, 1, tz=config.eastern_timezone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=custom_methods.get_report_params,
            replicon_conn_id=config.replicon_conn_id,
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_report_data',
            no_task='send_nodata_email'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        upload_reportdata_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reportdata_to_sftp',
            content="{{ result('load_report_data') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.filepath + config.report_name + " - "
            '{{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}' +
            "_" + "{{ dag_run_ecid() | replace(':', '-')}} 02_00 AM.csv",
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Punch Timesheet Details Report Extract Completed - {{ current_time("%d%m%Y%H%M%S") }}',
            html_content="templates/emails/success_email.html",
        )

        send_nodata_email = rail.EmailOperator(
            task_id='send_nodata_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Punch Timesheet Details Report Extract skipped - {{ current_time("%d%m%Y%H%M%S") }}',
            html_content="templates/emails/nodata_email.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "exportfile_name": config.report_name + " - "
                '{{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}' +
                "{{ dag_run_ecid() | replace(':', '-')}} 02_00 AM.csv"
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed >> rail.Label('No') >> report_has_data >> rail.Label(
            'Yes') >> load_report_data >> upload_reportdata_to_sftp >> send_success_email >> log_to_sumo >> can_fail_dag
        can_fail_dag >> rail.Label('Yes') >> fail_dagrun
        report_has_data >> rail.Label('No') >> send_nodata_email
        is_report_failed >> rail.Label('Yes') >> fail_report_generation

    return dag


rail.for_each_instance(create_main_dag)
