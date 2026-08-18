import rail
from pendulum import datetime

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_report_export_to_audit_trail_master_{config.instance}',
        description=f'Pwcfr_report_export_to_audit_trail {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2023, 5, 1, tz=config.time_zone),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            }
        )

        log_modified_daterange_filter = rail.PythonOperator(
            task_id='log_modified_daterange_filter',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'ModifiedOnUtcDateRangeFilter', 'uri', null)
        )

        generate_report = rail.run_report2(
            group_id='generate_report_data',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [{
                            "reportFilterUri": "{{result('log_modified_daterange_filter')}}",
                            "value": "Last7Days"
                        },
                            {
                            "reportFilterUri": "{{result('log_modified_daterange_filter')}}",
                            "value": "null"
                        },
                            {
                            "reportFilterUri": "{{result('log_modified_daterange_filter')}}",
                            "value": "null"
                        }],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
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
            remote_filepath=config.input_filepath + '/TO_AT_replicon' +
            '{{current_time("%d%m%Y")}}' + '.csv'
        )

        stop_job = rail.EmptyOperator(
            task_id='stop_job',
        )

        get_report_details >> run_report_entry
        run_report_exit >> log_modified_daterange_filter
        log_modified_daterange_filter >> generate_report >> if_payload_contains_error
        if_payload_contains_error >> rail.Label('Yes') >> stop_job_with_error
        if_payload_contains_error >> rail.Label('No') >> if_payload_has_data
        if_payload_has_data >> rail.Label('Yes') >> upload_file_to_sftp
        if_payload_has_data >> rail.Label('No') >> stop_job

        return dag


rail.for_each_instance(create_dag)
