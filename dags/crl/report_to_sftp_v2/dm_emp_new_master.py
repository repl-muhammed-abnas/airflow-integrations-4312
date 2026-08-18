import rail
from pendulum import datetime
from crl.report_to_sftp_v2.utlis import python_callable


# Report configuration
COLUMNS = ['Employee ID', 'User Name', 'Entry Date', 'Pay Code Name', 'Pay Code Code', 
          'Pay Code Description', 'Pay Code Hours', 'Timesheet Period', 'Employee Status', 
          'Employee Type', 'Pay Type', 'Default Activity', 'Standard Hours', 'Company Code', 
          'Location (Current) (Full Path)', 'Location', 'Business Segment', 'Functional Segment', 
          'Business Unit', 'Business Unit Code', 'Cost Center', 'Bus Area', 
          'Cost Center (Current) (Full Path)', 'Profit Center', 'Department', 'Department Code', 
          'Approval Status', 'Approval Date/Time', 'User Supervisor Name (Current)', 
          'User Supervisor Email address']

DATE_COLS = ['Entry Date', 'Timesheet Period', 'Approval Date/Time']


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.dm_emp_new_master_dag_id,
        description=f'CharlesRiverLaboratoriesSandbox DM_EMP_New Report Export',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 8, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_dm_emp_new,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        get_filename = rail.PythonOperator(
            task_id='get_filename',
            python_callable=python_callable.get_file_name,
            op_args=[config.DM_EMP_New_Filename, config.time_zone]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.DM_EMP_New_Report
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
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
            test="{{ result('run_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='no_data',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        process_and_convert_to_csv = rail.WriteCSVFileOperator(
            task_id='process_and_convert_to_csv',
            source="{{ result('load_report_data') }}",
            header=COLUMNS,
            row=lambda item: [python_callable.transform_report_row(item, DATE_COLS).get(col, '') for col in COLUMNS] if item else [''] * len(COLUMNS)
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        upload_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_report_to_sftp',
            content="{{ result('process_and_convert_to_csv') }}",
            remote_filepath=config.extract_report_file_path + '/{{ result("get_filename").filename }}'
        )

        upload_report_to_second_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_report_to_second_sftp',
            content="{{ result('process_and_convert_to_csv') }}",
            remote_filepath=config.archive_filepath + '/{{ result("get_filename").archive_filename }}'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'filename': '{{ result("get_filename").filename }}'
            }
        )

        get_filename >> get_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
        "Yes") >> load_report_data >> process_and_convert_to_csv >> upload_report_to_sftp >> upload_report_to_second_sftp >> log_to_sumo
        report_has_data >> rail.Label("No") >> no_data

    return dag


rail.for_each_instance(create_main_airflow_dag)
