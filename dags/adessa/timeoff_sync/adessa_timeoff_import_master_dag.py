
from datetime import timedelta
from adessa.timeoff_sync.utils import request_payload
from adessa.timeoff_sync.utils import response_filters
from adessa.timeoff_sync.tasks.send_logs import get_send_logs
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'adessa_timeoff_sync_adessa_timeoff_import_master_{config.instance}',
        description=f'Adessa Timeoff import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            poke_interval=600,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task='download_file',
            no_task='archive_invalid_file'
        )

        archive_invalid_file = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file',
            new_filename=config.archive_filepath + '/archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath + '/archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_file') }}",
            delimiter=';'
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id='create_input_collection',
            source='{{ result("parse_csv") }}',
            columns={
                "SFID": "sfid",
                "Date": "date",
                "Time-Off type": "timeoff_type",
                "Type": "type",
                "Duration": "duration",
                "Start Time": "start_time",
                "End Time": "end_time",
                "Approval Status": "approval_status",
                "Deletion Marker": "deletion_marker"
            },
            name='inputdata'
        )

        if_input_records_exists = rail.IfOperator(
            task_id='if_input_records_exists',
            test='{{ result("create_input_collection", "length") > 0 }}',
            yes_task="get_report_details",
            no_task="finish"
        )

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

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_expected_columns"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_has_data',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id = "fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_users_report_data',
            no_task= 'finish'
        )

        load_users_report_data = rail.LoadCSVFileOperator(
            task_id='load_users_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        users_report_data_collection = rail.CreateCollectionOperator(
            task_id='users_report_data_collection',
            source="{{ result('load_users_report_data') }}",
            name='usersdetails'
        )

        get_all_timeofftypes=rail.RepliconServiceOperator(
            task_id='get_all_timeofftypes',
            endpoint="/services/TimeOffTypeListService1.svc/GetData",
            data=request_payload.get_all_timeoff_types_payload,
            response_filter=response_filters.filter_timeoff_types
        )

        query_valid_input_records = rail.QueryCollectionOperator(
            task_id='query_valid_input_records',
            query="SELECT * FROM inputdata WHERE NULLIF(sfid, '') IS NOT NULL AND NULLIF(date,'') IS NOT NULL"
        )

        is_valid_records_exists = rail.IfOperator(
            task_id='is_valid_records_exists',
            test='{{ result("query_valid_input_records", "length") > 0 }}',
            yes_task='trigger_timeoff_import_child',
            no_task='process_logs'
        )

        query_invalid_input_records = rail.QueryCollectionOperator(
            task_id='query_invalid_input_records',
            query="SELECT * FROM inputdata WHERE NULLIF(sfid, '') IS NULL OR NULLIF(date,'') IS NULL"
        )

        is_invalid_records_exists = rail.IfOperator(
            task_id='is_invalid_records_exists',
            test='{{ result("query_invalid_input_records", "length") > 0 }}',
            yes_task='log_skipped_records',
            no_task='process_logs'
        )

        log_skipped_records = rail.WriteLogOperator(
            task_id='log_skipped_records',
            log='{{ result("create_log") }}',
            items='{{ result("query_invalid_input_records") }}',
            message=lambda item: 'Skipped | ' + "Employee ID is not present" if not item["sfid"] else ("Booking date is not present." if not item["date"] else null),
            severity="Skipped",
            properties=lambda item: {
                "childjobid": '{{ dag_run_ecid() }}',
                "employeeid": item["sfid"],
                "timeofftype": item["timeoff_type"],
                "startdate": item["date"],
                "action": item["deletion_marker"],
                "status": 'Skipped | ' + "Employee ID is not present" if not item["sfid"] else ("Booking date is not present." if not item["date"] else null)
            }
        )

        trigger_timeoff_import_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_import_child',
            retries=0,
            items='{{ result("query_valid_input_records") }}',
            trigger_dag_id=f'adessa_timeoff_sync_import_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_conf_payload
        )

        wait_for_timeoff_import_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_import_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_import_child") }}'
        )

        process_logs = rail.EmptyOperator(
            task_id='process_logs'
        )

        send_logs_enter, send_logs_exit = get_send_logs(config)

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> is_csv >> rail.Label("Yes") >> download_file
        is_csv >> rail.Label("No") >> archive_invalid_file
        download_file >> archive_file >> create_log >> parse_csv >> create_input_collection >> if_input_records_exists
        download_file >> was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        if_input_records_exists >> rail.Label('Yes') >> get_report_details >> run_report_entry
        if_input_records_exists >> rail.Label('No') >> finish
        run_report_exit >> is_report_failed >> rail.Label('Yes') >> fail_report_generation
        run_report_exit >> is_report_failed >> rail.Label('No') >> report_has_expected_columns
        report_has_expected_columns >> rail.Label('No') >> fail_invalid_report_colums
        report_has_expected_columns >> rail.Label('Yes') >> report_has_data
        report_has_data >> rail.Label('Yes') >> load_users_report_data >> users_report_data_collection \
            >> get_all_timeofftypes
        report_has_data >> rail.Label('No') >> finish
        get_all_timeofftypes >> query_valid_input_records >> is_valid_records_exists
        get_all_timeofftypes >> query_invalid_input_records >> is_invalid_records_exists

        is_valid_records_exists >> rail.Label("Yes") >> trigger_timeoff_import_child >> wait_for_timeoff_import_child >> process_logs
        is_valid_records_exists >> rail.Label("No") >> process_logs

        is_invalid_records_exists >> rail.Label("Yes") >> log_skipped_records >> process_logs
        is_invalid_records_exists >> rail.Label("No") >> process_logs

        process_logs >> send_logs_enter
        send_logs_exit >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
