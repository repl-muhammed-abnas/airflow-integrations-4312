from pendulum import datetime
import rail

from pike.report_to_ftp.utils import request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pike_quick_base_labor_entries_report_export_master_dag_{config.instance}",
        description=f"PIKE Quick Base Labor Entries Report Export Master Dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.mst_time_zone),
        schedule_interval=config.quick_base_export_schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.quick_base_report_name,
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

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('load_csv') }}",
            columns={
                "Week End Date (Timesheet End Date)": "week_end_date",
                "User Name": "user_name",
                "Billing Rate Name": "billing_rate_name",
                "Project Name": "project_name",
                "Project Code": "project_code",
                "Task Code": "task_code",
                "Activity Name": "activity_name",
                "Hours": "hours",
                "Comments": "comments",
                "User Supervisor Name (Current)": "user_supervisor_name_current"
            },
            name='time_data'
        )

        labor_data = rail.PythonOperator(
            task_id='labor_data',
            python_callable=request_payload.read_collection
        )

        write_csv = rail.WriteCSVFileOperator(
            task_id='write_csv',
            source="{{ result('create_report_collection') }}",
            header=["Week End Date (Timesheet End Date)", "User Name", "Billing Rate Name",
                    "Project Name", "Project Code", "Task Code", "Activity Name", "Hours",
                    "Comments", "User Supervisor Name (Current)", "Index"],
            row=request_payload.get_csv_rows,
            lineterminator='\n'
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content='{{ result("write_csv") }}',
            remote_filepath=config.quick_base_export_path,
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_my_report_entry
        run_my_report_exit >> report_has_data

        report_has_data >> rail.Label("Yes") >> load_csv >> create_report_collection >> labor_data >> write_csv >> upload_csv_to_sftp
        report_has_data >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
