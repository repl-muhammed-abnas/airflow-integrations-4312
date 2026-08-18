import rail
from pendulum import datetime
from datetime import timedelta


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_shift_users_child_dag_id,
        description="TransparentBPO Schedule Sync shift users",
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
            report_name=config.shift_report_name,
        )

        get_report_details = rail.RepliconServiceOperator(
            task_id="get_report_details",
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result('get_report_uri')['uri']
            },
            data_handler=lambda response: {
                "shiftnamefilter_uri": rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'ShiftNameFilter', 'uri', '')
            }
        )

        get_report_filter_value_details = rail.RepliconServiceOperator(
            task_id="get_report_filter_value_details",
            endpoint="/services/reportService1.svc/GetAllFilterValueDetailsForFilter",
            data=lambda: {
                "reportUri": rail.result('get_report_uri')['uri'],
                "reportFilter": rail.result('get_report_details')['shiftnamefilter_uri']
            },
        )

        def get_required_report_params():
            all_filter_values = rail.result('get_report_filter_value_details')
            shiftnamefilter_uri = rail.result('get_report_details')[
                'shiftnamefilter_uri']
            filter_values = []
            for item in all_filter_values:
                value = item.get("value")
                if value != "NONE":
                    item.update({
                        "reportFilterUri": shiftnamefilter_uri
                    })
                    filter_values.append(item)
            return {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_uri')['uri'],
                        "filterValues": filter_values,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

        run_report_shift_users = rail.run_report2(
            group_id='run_report_shift_users',
            report_params=get_required_report_params
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report_shift_users.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report_shift_users.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report_shift_users.get_report_result', 'has_data') }}",
            yes_task='report_has_expected_columns',
            no_task='no_data'
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            test=lambda: rail.result(
                'run_report_shift_users.get_report_result')['reportGenerationResults'][0]['payload'].startswith(config.shift_user_expected_report_columns),
            no_task='fail_invalid_report_colums',
            yes_task='list_reference_files'
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id="fail_invalid_report_colums",
            message="Report column does not match"
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            paths=[config.shift_user_reference_filepath]
        )

        foreach_file_in_reference_folder = rail.ForEachOperator(
            task_id='foreach_file_in_reference_folder',
            items=lambda: rail.result(
                'list_reference_files')[config.shift_user_reference_filepath],
            start_task='if_file_name_starts_with_shift',
            end_task='foreach_file_in_reference_folder_end'
        )

        if_file_name_starts_with_shift = rail.IfOperator(
            task_id='if_file_name_starts_with_shift',
            test=lambda:  rail.result('foreach_file_in_reference_folder')['name'].startswith(
                config.shift_user_reference_filename_startswith),
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
            remote_filepath=config.shift_user_reference_filepath +
            "/{{ result('get_reference_filename')}}"
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            new_filename=config.shift_user_reference_archive_filepath + (
                "/{{ result('get_reference_filename')}}_" + "{{current_time_in_specified_tz('" + config.time_zone + "', '%Y%m%dT%H%M%S')}}"),
            existing_filename=config.shift_user_reference_filepath + (
                "/{{ result('get_reference_filename')}}")
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}"
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source="{{ result('parse_reference_file') }}",
            name="shift_user_reference",
        )

        parse_report_file = rail.LoadCSVFileOperator(
            task_id="parse_report_file",
            document="{{result('run_report_shift_users.get_report_result').reportGenerationResults[0].payload}}"
        )

        create_reportfile_collection = rail.CreateCollectionOperator(
            task_id='create_reportfile_collection',
            source="{{result('parse_report_file')}}",
            name="shift_user_report",
        )

        query_list_validated_delta_records = rail.QueryCollectionOperator(
            task_id='query_list_validated_delta_records',
            name='validated_delta_records',
            query="""SELECT * FROM shift_user_report 
                WHERE uniquevalue NOT IN(SELECT uniquevalue FROM shift_user_reference)"""
        )

        if_query_shift_users_blank = rail.IfOperator(
            task_id='if_query_shift_users_blank',
            test=lambda: rail.result(
                "query_list_validated_delta_records", "length") < 1,
            yes_task='upload_new_reference_file',
            no_task='postrepliconschedule_bamboohr_call_child'
        )

        postrepliconschedule_bamboohr_call_child = rail.TriggerDagRunForEachItemOperator(
            task_id='postrepliconschedule_bamboohr_call_child',
            retries=0,
            items='{{ result("query_list_validated_delta_records") }}',
            trigger_dag_id=config.post_to_bamboohr_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'employee_id': item['Employee_ID'],
                'current_schedule': item['Shift_Name'],
                'uer_uri': item['UserUri'],
                'user_name': item['User_Name'],
                'schedule_update_logs': dag_run.conf['schedule_update_logs'],
                'bamboohr_id': item['Bamboo_HR_ID']
            },
        )

        wait_for_postrepliconschedule_bamboohr_call_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_postrepliconschedule_bamboohr_call_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("postrepliconschedule_bamboohr_call_child") }}'
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content="{{result('run_report_shift_users.get_report_result').reportGenerationResults[0].payload}}",
            remote_filepath=config.shift_user_reference_filepath + (
                '/shiftusers_{{ current_time_in_specified_tz("' + config.time_zone + '", "%Y%m%dT%H%M%S") }}.csv'),
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            python_callable=lambda: rail.result(
                "query_list_validated_delta_records", "length")
        )

        get_report_uri >> get_report_details >> get_report_filter_value_details >> run_report_shift_users >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_data >> rail.Label("No") >> no_data

        report_has_expected_columns >> rail.Label(
            "Yes") >> list_reference_files
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_colums

        list_reference_files >> foreach_file_in_reference_folder

        foreach_file_in_reference_folder >> if_file_name_starts_with_shift

        if_file_name_starts_with_shift >> rail.Label(
            "Yes") >> get_reference_filename >> foreach_file_in_reference_folder_end
        if_file_name_starts_with_shift >> rail.Label(
            "No") >> foreach_file_in_reference_folder_end

        foreach_file_in_reference_folder >> foreach_file_in_reference_folder_end >> if_file_not_present

        if_file_not_present >> rail.Label(
            "Yes") >> fail_with_reference_file_missing
        if_file_not_present >> rail.Label(
            "No") >> download_reference_file >> archive_reference_file >> parse_reference_file

        parse_reference_file >> create_referencefile_collection >> parse_report_file >> create_reportfile_collection \
            >> query_list_validated_delta_records
        query_list_validated_delta_records >> if_query_shift_users_blank

        if_query_shift_users_blank >> rail.Label(
            "Yes") >> upload_new_reference_file

        if_query_shift_users_blank >> rail.Label(
            "No") >> postrepliconschedule_bamboohr_call_child >> wait_for_postrepliconschedule_bamboohr_call_child
        wait_for_postrepliconschedule_bamboohr_call_child >> upload_new_reference_file >> final_response_from_dag

    return dag


rail.for_each_instance(create_child_dag)
