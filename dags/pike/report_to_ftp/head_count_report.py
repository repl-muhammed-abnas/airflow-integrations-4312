from pendulum import datetime
import rail

from pike.report_to_ftp.utils import request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pike_head_count_report_export_master_dag_{config.instance}",
        description=f"PIKE Head Count Report Export Master Dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.est_time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.head_count_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_report_generate_batch_payload
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_csv',
            no_task= 'finish'
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content='{{ result("load_csv") }}',
            remote_filepath=config.head_count_export_path +
                "Headcount Report - Telecom-{{current_time_in_specified_tz('Canada/Eastern', '%m_%d_%YT%H_%M')}}.csv",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_my_report_entry
        run_my_report_exit >> report_has_data

        report_has_data >> rail.Label("Yes") >> load_csv >> upload_csv_to_sftp
        report_has_data >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
