
import rail
from datetime import timedelta


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_scheduled_users_child_dag_id,
        description="TransparentBPO Schedule Sync Process scheduled users",
        company_key=config.company_key,
        max_active_runs=config.max_active_child_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        get_report_uri = rail.RepliconReportDetailsOperator(
            task_id='get_report_uri',
            report_name=config.shedule_report_name,
        )

        get_report_details = rail.run_report2(
            group_id='get_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_uri').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("get_report_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('get_report_details.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('get_report_details.get_report_result', 'has_data') }}",
            yes_task='report_has_expected_columns',
            no_task='no_data'
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            test=lambda: rail.result(
                'get_report_details.get_report_result')['reportGenerationResults'][0]['payload'].startswith(config.scheduled_user_expected_report_columns),
            no_task='fail_invalid_report_colums',
            yes_task='list_reference_files'
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id="fail_invalid_report_colums",
            message="Report column does not match"
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            paths=[config.schedule_user_reference_filepath]
        )

        foreach_file_in_reference_folder = rail.ForEachOperator(
            task_id='foreach_file_in_reference_folder',
            items=lambda: rail.result(
                'list_reference_files')[config.schedule_user_reference_filepath],
            start_task='if_file_name_starts_with_schedule',
            end_task='foreach_file_in_reference_folder_end'
        )

        if_file_name_starts_with_schedule = rail.IfOperator(
            task_id='if_file_name_starts_with_schedule',
            test=lambda:  rail.result('foreach_file_in_reference_folder')['name'].startswith(
                config.schedule_user_reference_filename_startswith),
            yes_task='get_reference_filename',
            no_task='foreach_file_in_reference_folder_end'
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: rail.result(
                'foreach_file_in_reference_folder')['name']
        )

        foreach_file_in_reference_folder_end = rail.EmptyOperator(
            task_id='foreach_file_in_reference_folder_end'
        )

        if_file_not_present = rail.IfOperator(
            task_id='if_file_not_present',
            test=lambda: bool(not (rail.result('get_reference_filename'))),
            yes_task="fail_with_reference_file_missing",
            no_task="download_reference_file",
        )

        fail_with_reference_file_missing = rail.FailOperator(
            task_id='fail_with_reference_file_missing',
            message='''Reference file missing'''
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.schedule_user_reference_filepath +
            "/{{ result('get_reference_filename')}}"
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            new_filename=config.schedule_user_reference_archive_filepath +
            "/{{ result('get_reference_filename')}}_" +
            "{{current_time_in_specified_tz('" +
            config.time_zone + "', '%Y%m%dT%H%M%S')}}",
            existing_filename=config.schedule_user_reference_filepath +
            "/{{ result('get_reference_filename')}}"
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}"
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source="{{ result('parse_reference_file') }}",
            name="scheduled_user_reference",
        )

        parse_report_file = rail.LoadCSVFileOperator(
            task_id="parse_report_file",
            document="{{result('get_report_details.get_report_result').reportGenerationResults[0].payload}}"
        )

        create_reportfile_collection = rail.CreateCollectionOperator(
            task_id='create_reportfile_collection',
            source="{{result('parse_report_file')}}",
            name="scheduled_user_report",
        )

        query_list_validated_delta_records = rail.QueryCollectionOperator(
            task_id='query_list_validated_delta_records',
            name='validated_delta_records',
            query="""SELECT * FROM scheduled_user_report 
                WHERE uniquevalue NOT IN(SELECT uniquevalue FROM scheduled_user_reference)"""
        )

        query_non_shift_users = rail.QueryCollectionOperator(
            task_id='query_non_shift_users',
            name='non_shift_users',
            query='''SELECT * 
                FROM validated_delta_records 
                WHERE(Schedule_Name__Current_ != "Shift Schedule")'''
        )

        if_query_non_shift_users_blank = rail.IfOperator(
            task_id='if_query_non_shift_users_blank',
            test=lambda: rail.result("query_non_shift_users", "length") < 1,
            yes_task='upload_new_reference_file',
            no_task='postrepliconschedule_bamboohr_call_child'
        )

        postrepliconschedule_bamboohr_call_child = rail.TriggerDagRunForEachItemOperator(
            task_id='postrepliconschedule_bamboohr_call_child',
            retries=0,
            items='{{ result("query_non_shift_users") }}',
            conf=lambda item, dag_run: {
                'employee_id': item['Employee_ID'],
                'current_schedule': item['Schedule_Name__Current_'],
                'uer_uri': item['UserUri'],
                'user_name': item['User_Name'],
                'schedule_update_logs': dag_run.conf['schedule_update_logs'],
                'bamboohr_id': item['Bamboo_HR_ID']
            },
            trigger_dag_id=config.post_to_bamboohr_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_postrepliconschedule_bamboohr_call_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_postrepliconschedule_bamboohr_call_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("postrepliconschedule_bamboohr_call_child") }}'
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload }}",
            remote_filepath=config.schedule_user_reference_filepath +
            '/scheduleusers_{{ current_time_in_specified_tz("' +
            config.time_zone + '", "%Y%m%dT%H%M%S") }}.csv',
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            python_callable=lambda: rail.result(
                "query_non_shift_users", "length")
        )

    get_report_uri >> get_report_details >> is_report_failed >> rail.Label(
        "Yes") >> fail_report_generation
    is_report_failed >> rail.Label("No") >> report_has_data

    report_has_data >> rail.Label("Yes") >> report_has_expected_columns
    report_has_data >> rail.Label("No") >> no_data

    report_has_expected_columns >> rail.Label("Yes") >> list_reference_files
    report_has_expected_columns >> rail.Label(
        "No") >> fail_invalid_report_colums

    list_reference_files >> foreach_file_in_reference_folder >> if_file_name_starts_with_schedule

    if_file_name_starts_with_schedule >> rail.Label(
        "Yes") >> get_reference_filename >> foreach_file_in_reference_folder_end
    if_file_name_starts_with_schedule >> rail.Label(
        "No") >> foreach_file_in_reference_folder_end

    foreach_file_in_reference_folder >> foreach_file_in_reference_folder_end >> if_file_not_present

    if_file_not_present >> rail.Label(
        "Yes") >> fail_with_reference_file_missing
    if_file_not_present >> rail.Label("No") >> download_reference_file

    download_reference_file >> archive_reference_file >> parse_reference_file >> create_referencefile_collection >> parse_report_file >> create_reportfile_collection >> query_list_validated_delta_records
    query_list_validated_delta_records >> query_non_shift_users >> if_query_non_shift_users_blank

    if_query_non_shift_users_blank >> rail.Label(
        "Yes") >> upload_new_reference_file
    if_query_non_shift_users_blank >> rail.Label(
        "No") >> postrepliconschedule_bamboohr_call_child

    postrepliconschedule_bamboohr_call_child >> wait_for_postrepliconschedule_bamboohr_call_child
    wait_for_postrepliconschedule_bamboohr_call_child >> upload_new_reference_file

    upload_new_reference_file >> final_response_from_dag

    return dag

rail.for_each_instance(create_child_dag)
