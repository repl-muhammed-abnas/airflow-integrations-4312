from datetime import timedelta
from pendulum import datetime
import rail

from victoriashipyards.auto_shift_assignment.new_user.utils import request_payload

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'victoriashipyards_assign_shift_to_new_users_master_V3.0_{config.instance}',
        description=f'victoriashipyards_assign shift to new users_master V3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.victoriashipyards_sftp_conn_id,
        },
    ) as dag:

        def get_column_names():
            return {
                'Login Name':'loginname',
                'User URI':'useruri',
                'defaultshift': 'defaultshift',
                'Schedule Name (Current)':'schedulenamecurrent',
                'User Name':'username',
                'VSY Default Shift':'vsy',
                'VDC Default Shift':'vdc'}

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_shift_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_shift_report_generation',
            no_task='report_has_data'
        )

        fail_shift_report_generation = rail.FailOperator(
            task_id="fail_shift_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_report_data',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='No Data for Shift Users'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.victoriashipyards_sftp_conn_id,
            remote_filepath=config.reference_filepath,
        )

        create_todayfile_collection = rail.CreateCollectionOperator(
            task_id='create_todayfile_collection',
            name='todaysfile',
            source="{{ result('load_report_data') }}",
            columns=get_column_names()
        )

        get_userdata_to_process = rail.QueryCollectionOperator(
            task_id = 'get_userdata_to_process',
            name='todaysfiledata',
            # pylint: disable=line-too-long
            query = 'SELECT * FROM todaysfile WHERE schedulenamecurrent = "Shift Schedule" AND defaultshift NOT IN ("NA_Non-Exempt", "EA_Exempt", "Not Applicable")'
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}"
        )

        create_reference_file_collection = rail.CreateCollectionOperator(
            task_id='create_reference_file_collection',
            name="referencefile",
            source="{{result('parse_reference_file')}}",
            columns=get_column_names()
        )

        get_shift_user_in_reference_file = rail.QueryCollectionOperator(
            task_id = 'get_shift_user_in_reference_file',
            name='referencefiledata',
            # pylint: disable=line-too-long
            query = 'SELECT * FROM referencefile WHERE schedulenamecurrent = "Shift Schedule" AND defaultshift NOT IN ("NA_Non-Exempt", "EA_Exempt", "Not Applicable")',
        )

        get_new_users = rail.QueryCollectionOperator(
            task_id='get_new_users',
            query='SELECT * from todaysfiledata WHERE useruri NOT IN(SELECT DISTINCT useruri from referencefiledata)'
        )

        has_userdata_to_process = rail.IfOperator(
            task_id="has_userdata_to_process",
            test="{{ result('get_new_users', 'length') > 0 }}",
            yes_task='process_shifts',
            no_task='finish'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        process_shifts = rail.TriggerDagRunForEachItemOperator(
            task_id='process_shifts',
            retries=0,
            items="{{ result('get_new_users')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'victoriashipyards_default_shift_assignment_per_new_user_V3.0_{config.instance}',
            conf=request_payload.get_shift_assignment_data_for_daily_update
        )

        upload_reference_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reference_file_to_sftp',
            content="{{ result('load_report_data')}}",
            remote_filepath=config.reference_filepath
        )

        log_number_of_users_data_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_number_of_users_data_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                'Number of Users' : '{{ result("create_todayfile_collection","length")}}',
                'Number of New Users' : '{{ result("get_new_users","length")}}',
                'Number of Old Users' : '{{ result("create_reference_file_collection","length")}}'
            }
        )

        get_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed
        is_report_failed >> rail.Label("Yes") >> fail_shift_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data
        # pylint: disable=line-too-long
        report_has_data >> rail.Label("Yes") >> load_report_data >> download_reference_file >> create_todayfile_collection >> get_userdata_to_process >> parse_reference_file >> create_reference_file_collection >> get_shift_user_in_reference_file >> get_new_users >> has_userdata_to_process
        has_userdata_to_process >> rail.Label("Yes") >> process_shifts >> upload_reference_file_to_sftp >> log_number_of_users_data_to_sumo
        has_userdata_to_process >> rail.Label("No") >> finish
        report_has_data >> rail.Label("No") >> fail_no_report_data

    return dag
rail.for_each_instance(create_dag)
