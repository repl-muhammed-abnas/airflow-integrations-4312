from datetime import datetime as dt, timedelta
from pendulum import datetime, now
import pendulum
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description=f"Calendar Year Distance Extract for Netherlands",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.europe_timezone),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs_master
    ) as dag:

        def is_scheduled_run_date(dag_run, config):
            """
            Validates if the DAG is running on the scheduled dates (January 15 or February 15).
            Can be bypassed with skip_rundate_validation in dag_run.conf
            """
            # Allow manual override for testing/manual runs
            if dag_run.conf.get('skip_rundate_validation', False):
                return True

            # Get current date in Europe/Paris timezone for schedule validation
            current_date_in_tz = now(tz=config.europe_timezone)

            # Check if current date is January 15 or February 15
            current_day = current_date_in_tz.day
            current_month = current_date_in_tz.month

            # Should only run on 15th day of January (1) or February (2)
            return current_day == 15 and current_month in [1, 2]

        validate_schedule = rail.IfOperator(
            task_id='validate_schedule',
            test=lambda dag_run: is_scheduled_run_date(dag_run, config),
            yes_task='process_start_time',
            no_task='skip_unscheduled_run'
        )

        skip_unscheduled_run = rail.EmptyOperator(
            task_id='skip_unscheduled_run'
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda: pendulum.now(
                config.europe_timezone).strftime("%Y%m%d%H%M%S")
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: "Yearly_Distance_Extract_" +
            rail.result("process_start_time") + "_NLD.csv"
        )

        get_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details',
            report_name=config.report_name,
        )

        def last_day_of_month(date):
            if date.month == 12:
                return date.replace(day=31)
            return (date.replace(month=date.month + 1, day=1) - timedelta(days=1))

        # pylint: disable=line-too-long
        process_previous_calendar_year_extract_upload_file = rail.TriggerDagRunForEachItemOperator(
            task_id='process_previous_calendar_year_extract_upload_file',
            retries=0,
            items=list(range(0, config.batch_size)),
            trigger_dag_id=config.upload_file_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'index':  item,
                'batch_size': config.batch_size,
                'report_uri': rail.result('get_specific_report_details')['uri'],
                'report_filter_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_specific_report_details')["filterConfiguration"]["enabledFilters"], 'displayText', config.filter_name, 'uri'),
                'start_date':  str(dt(dt.today().year - 1, int(item * int(12/config.batch_size)) + 1, 1).strftime("%m/%d/%Y")),
                'end_date': str((last_day_of_month(dt(dt.today().year - 1, int((item + 1) * int(
                    12/config.batch_size)), 1)).strftime("%m/%d/%Y"))),
                'sequence_no': item + 1,
                'file_name': rail.result('get_file_name')
            })

        wait_to_process_previous_calendar_year_extract = rail.WaitForDagRunsSensor(
            task_id='wait_to_process_previous_calendar_year_extract',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_previous_calendar_year_extract_upload_file") }}',
        )

        get_data_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='get_data_from_child',
            dag_runs='{{ result("process_previous_calendar_year_extract_upload_file") }}',
            dagrun_task_id='report_payload_to_csv',
            flatten=True,
        )

        child_has_data = rail.IfOperator(
            task_id="child_has_data",
            test='{{result("get_data_from_child") | is_truthy }}',
            yes_task='dummy_process_each_report_artifact',
            no_task='finish_export'
        )

        dummy_process_each_report_artifact = rail.EmptyOperator(
            task_id='dummy_process_each_report_artifact'
        )

        foreach_report_batch_artifact = rail.ForEachOperator(
            task_id="foreach_report_batch_artifact",
            items=lambda: rail.result('get_data_from_child'),
            start_task='create_report_data_collection',
            end_task='foreach_report_batch_artifact_end'
        )

        create_report_data_collection = rail.CreateCollectionOperator(
            task_id="create_report_data_collection",
            name='report_data_batch_{{result("foreach_report_batch_artifact", "index")}}',
            source=lambda: rail.result("foreach_report_batch_artifact")
        )

        foreach_report_batch_artifact_end = rail.EmptyOperator(
            task_id='foreach_report_batch_artifact_end',
        )

        get_query_for_combining_available_report_collections = rail.PythonOperator(
            task_id="get_query_for_combining_available_report_collections",
            python_callable=lambda: ' UNION '.join(
                [f'SELECT * FROM report_data_batch_{i}' for i in range(len(rail.result('get_data_from_child')))])
        )

        get_final_report_data_collection = rail.QueryCollectionOperator(
            task_id="get_final_report_data_collection",
            query="{{result('get_query_for_combining_available_report_collections')}}",
            name='final_report_data_collection'
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        final_report_data = rail.QueryCollectionOperator(
            task_id="final_report_data",
            query='''SELECT
                    TransactionDate,
                    TimeEntryID,
                    PartyID,
                    ResourceGrade,
                    LegalEntityPartyID,
                    WorkdayID,
                    TimesheetStartDate,
                    TimesheetEndDate,
                    REPLACE(Mileage, ',', '') AS Mileage,
                    ChargeCode,
                    WorkItemType,
                    Comments,
                    REPLACE(Distance, ',', '') AS Distance,
                    REPLACE(DistanceCarotherfuel, ',', '') AS DistanceCarotherfuel,
                    REPLACE(DistancePublictransport, ',', '') AS DistancePublictransport,
                    REPLACE(DistanceCar100_electric, ',', '') AS DistanceCar100_electric,
                    REPLACE(DistanceCarpetrol, ',', '') AS DistanceCarpetrol,
                    REPLACE(DistanceCar_plugin_hybrid, ',', '') AS DistanceCar_plugin_hybrid,
                    REPLACE(DistanceCardiesel, ',', '') AS DistanceCardiesel,
                    REPLACE(Distance_e__Bikeorwalking, ',', '') AS Distance_e__Bikeorwalking,
                    REPLACE(DistanceMotorbikepetrol, ',', '') AS DistanceMotorbikepetrol,
                    REPLACE(DistanceMotorbikeelectric, ',', '') AS DistanceMotorbikeelectric,
                    REPLACE(Distancescooterpetrol, ',', '') AS Distancescooterpetrol,
                    REPLACE(Distancescooterelectric, ',', '') AS Distancescooterelectric,
                    REPLACE(TotalDistance, ',', '') AS TotalDistance
                FROM final_report_data_collection
                WHERE
                    NULLIF(TransactionDate, '') IS NOT NULL AND
                    NULLIF(TRIM(TotalDistance), '') IS NOT NULL AND
                    ABS(CAST(REPLACE(TRIM(TotalDistance), ',', '') AS DECIMAL(10,2))) > 0.000 '''
        )

        final_data_to_csv = rail.WriteCSVFileOperator2(
            task_id="final_data_to_csv",
            source="{{ result('final_report_data') }}",
            header=[
                'TransactionDate', 'TimeEntryID', 'PartyID', 'ResourceGrade', 'LegalEntityPartyID', 'WorkdayID', 'TimesheetStartDate',
                'TimesheetEndDate', 'Mileage', 'ChargeCode', 'WorkItemType', 'Comments', 'Distance',
                'DistanceCarotherfuel', 'DistancePublictransport', 'DistanceCar100%electric',
                'DistanceCarpetrol', 'DistanceCar(plugin)hybrid', 'DistanceCardiesel', 'Distance(e-)Bikeorwalking', 'DistanceMotorbikepetrol',
                'DistanceMotorbikeelectric', 'Distancescooterpetrol', 'Distancescooterelectric', 'TotalDistance'
            ],
            thread_pool_size=config.thread_pool_size_write_csv,
            row=[
                '{{item.TransactionDate}}',
                '{{item.TimeEntryID}}',
                '{{item.PartyID}}',
                '{{item.ResourceGrade}}',
                '{{item.LegalEntityPartyID}}',
                '{{item.WorkdayID}}',
                '{{item.TimesheetStartDate}}',
                '{{item.TimesheetEndDate}}',
                '{{item.Mileage}}',
                '{{item.ChargeCode}}',
                '{{item.WorkItemType}}',
                '{{item.Comments}}',
                '{{item.Distance}}',
                '{{item.DistanceCarotherfuel}}',
                '{{item.DistancePublictransport}}',
                '{{item.DistanceCar100_electric}}',
                '{{item.DistanceCarpetrol}}',
                '{{item.DistanceCar_plugin_hybrid}}',
                '{{item.DistanceCardiesel}}',
                '{{item.Distance_e__Bikeorwalking}}',
                '{{item.DistanceMotorbikepetrol}}',
                '{{item.DistanceMotorbikeelectric}}',
                '{{item.Distancescooterpetrol}}',
                '{{item.Distancescooterelectric}}',
                '{{item.TotalDistance}}'
            ],
            execution_timeout=timedelta(
                hours=config.execution_timeout_write_csv),
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content='{{result("final_data_to_csv")}}',
            execution_timeout=timedelta(minutes=60),
            remote_filepath=config.output_file_path +
            "{{result('get_file_name')}}"
        )

        is_upload_file_to_different_path = rail.IfOperator(
            task_id="is_upload_file_to_different_path",
            test=config.is_upload_file_to_different_path_required,
            yes_task="upload_file_to_different_path",
            no_task="send_export_complete_email"
        )

        upload_file_to_different_path = rail.SFTPUploadFileOperator(
            task_id="upload_file_to_different_path",
            content='{{result("final_data_to_csv")}}',
            remote_filepath=config.alternate_file_path +
            '{{result("get_file_name")}}'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Previous Year Distance Extract for Netherlands -{{result('process_start_time')}}",
            html_content="export_complete_mail.html",
            params={
                'output_file_path': config.output_file_path
            }
        )

        validate_schedule >> rail.Label('Yes') >> process_start_time
        validate_schedule >> rail.Label('No') >> skip_unscheduled_run

        process_start_time >> get_file_name >> get_specific_report_details >> process_previous_calendar_year_extract_upload_file \
            >> wait_to_process_previous_calendar_year_extract >> get_data_from_child >> child_has_data
        child_has_data >> rail.Label("No") >> finish_export
        child_has_data >> rail.Label(
            "Yes") >> dummy_process_each_report_artifact >> foreach_report_batch_artifact

        foreach_report_batch_artifact >> create_report_data_collection >> foreach_report_batch_artifact_end

        foreach_report_batch_artifact >> foreach_report_batch_artifact_end >> get_query_for_combining_available_report_collections

        get_query_for_combining_available_report_collections >> get_final_report_data_collection >> final_report_data

        final_report_data >> final_data_to_csv >> upload_export_data_to_sftp >> is_upload_file_to_different_path

        is_upload_file_to_different_path >> rail.Label(
            "No") >> send_export_complete_email
        is_upload_file_to_different_path >> rail.Label(
            "Yes") >> upload_file_to_different_path >> send_export_complete_email

    return dag


rail.for_each_instance(create_main_dag)
