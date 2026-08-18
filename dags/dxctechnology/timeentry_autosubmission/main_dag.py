from datetime import timedelta, datetime
import pytz
import pendulum
import rail
from dxctechnology.timeentry_autosubmission.utils import custom_methods
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_time_entry_auto_submission_master_{config.instance}',
        description=f'DxcTechnology Time Entry Auto Submission Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        is_valid_schedule = rail.IfOperator(
            task_id="is_valid_schedule",
            test=lambda: (
                custom_methods.schedule_interval_sandbox(config.pacific_timezone)
                if "sandbox" in config.instance
                else custom_methods.schedule_interval(config.pacific_timezone)
            ),
            yes_task="dxc_timeentry_auto_submission_location_mapper_search_entries_2",
            no_task="delete_this_dagrun",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        dxc_timeentry_auto_submission_location_mapper_search_entries_2 = rail.PythonOperator(
            task_id='dxc_timeentry_auto_submission_location_mapper_search_entries_2',
            python_callable=lambda:  list(filter(lambda x: x["Country"] == config.country and x["Enabled"] ==
                                          "Yes", config.MAPPER))
        )

        def get_locations():
            records = rail.result('dxc_timeentry_auto_submission_location_mapper_search_entries_2') if rail.result(
                'dxc_timeentry_auto_submission_location_mapper_search_entries_2') else []
            result = '|'.join(item['Locations']
                              for item in records if item['Country'] == 'Africa')
            return result

        accumulate_list_items_3 = rail.SetVariableOperator(
            task_id='accumulate_list_items_3',
            name='values',
            append=False,
            value=lambda: {
                "today": datetime.now(pytz.timezone("Africa/Cairo")).strftime("%m/%d/%Y"),
                "enddate": datetime.now(pytz.timezone("Africa/Cairo")).strftime("%m/%d/%Y"),
                "startdate": (datetime.now(pytz.timezone("Africa/Cairo")) - timedelta(days=config.look_back_period_in_days)).strftime("%m/%d/%Y"),
                "locations": get_locations()
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint='/services/LocationService1.svc/GetEnabledLocations'
        )

        def get_filters():
            mapper_data = rail.result(
                'dxc_timeentry_auto_submission_location_mapper_search_entries_2')
            locations_data = rail.result('get_enabled_locations')
            current_location_filter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')[
                'filterConfiguration']['enabledFilters'], 'displayText', 'CurrentLocationFilter', 'uri', null)
            timesheet_period_filter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                    )['filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri', null)
            filters = [{
                "reportFilterUri": timesheet_period_filter,
                "value": null
            }, {
                "reportFilterUri": timesheet_period_filter,
                "value": rail.result('accumulate_list_items_3')['value']['startdate']
            }, {
                "reportFilterUri": timesheet_period_filter,
                "value": rail.result('accumulate_list_items_3')['value']['enddate']
            },
            {
                "reportFilterUri": timesheet_period_filter,
                "value": "Overlapped"
            },
                {
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                         )['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', null),
                "value": 0
            }]
            for data in mapper_data:
                filters.append({
                    "reportFilterUri": current_location_filter,
                    "value": ((rail.find_first_by_attr_and_get_attr(locations_data, 'displayText', data['Locations'], 'uri', '')).split(':'))[-1]
                })
            return filters

        load_users_data_from_report = rail.run_report2(
            group_id="load_users_data_from_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": get_filters(),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "persistedReportName": null
                    }
                ],
                "step_time": "120"
            }
        )

    has_load_users_data_from_report_data = rail.IfOperator(
        task_id='has_load_users_data_from_report_data',
        test='{{ result("load_users_data_from_report.get_report_result", "has_data") }}',
        yes_task='check_batch_error',
        no_task='end'
    )

    check_batch_error = rail.EmptyOperator(
        task_id='check_batch_error'
    )

    is_error_present_in_batch = rail.IfOperator(
        task_id='is_error_present_in_batch',
        test='{{ result("load_users_data_from_report.get_report_result").reportGenerationResults[0].error | is_truthy }}',
        yes_task='fail_with_error_log',
        no_task='load_users_data_from_report_payload_to_csv'
    )

    fail_with_error_log = rail.FailOperator(
        task_id='fail_with_error_log',
        message='{{ result("load_users_data_from_report.get_report_result").reportGenerationResults[0].error }}'
    )

    load_users_data_from_report_payload_to_csv = rail.LoadCSVFileOperator(
        task_id="load_users_data_from_report_payload_to_csv",
        document='{{ result("load_users_data_from_report.get_report_result").reportGenerationResults[0].payload }}'
    )

    timesheet_report_collection = rail.CreateCollectionOperator(
        task_id='timesheet_report_collection',
        name='timesheetreport',
        source='{{ result("load_users_data_from_report_payload_to_csv") }}',
        columns={
            'Timesheet Period': 'timesheetperiod',
                'User Name': 'username',
                'Employee ID': 'employeeid',
                'Validation Messages': 'validationmessages',
                'Approval Status': 'approvalstatus',
                'TimesheetUri': 'timesheeturi',
                'Timesheet Start Date': 'timesheetstartdate',
                'Timesheet End Date': 'timesheetenddate',
                'UserUri': 'useruri',
                'Employee Type (Current)': 'employeetype',
                'Employee type group': 'employeetypegroup',
                'Total Hrs (In Period)': 'totalhours',
                'daydiff': 'daydiff',
                'Total TimeOff Hrs (In Period)': 'totaltimeoffhours'
        }
    )

    query_collection_data = rail.QueryCollectionOperator(
        task_id="query_collection_data",
        query="""SELECT * FROM timesheetreport """,
    )

    query_timesheet_report_collection = rail.QueryCollectionOperator(
        task_id="query_timesheet_report_collection",
        # pylint: disable=line-too-long
        query="""SELECT * FROM timesheetreport WHERE employeetype != "Contractor" AND ((CAST (daydiff AS INTEGER) < -8)  AND totalhours > 0.01 AND totalhours != totaltimeoffhours)
            AND (validationmessages == "Null" OR LOWER(validationmessages) LIKE 'the entry for the day should not exceed a maximum%' OR
            LOWER(validationmessages) LIKE 'the entry for the day should not exceed a maximum%' OR
            LOWER(validationmessages) LIKE 'the total hours for the week should be greater than or equal to%' OR
            LOWER(validationmessages) LIKE 'ia perner id is not available. please contact the wbs owner.%' OR
            LOWER(validationmessages) LIKE 'ia perner id is not available. please contact the wbs owner.%' OR
            LOWER(validationmessages) LIKE 'you have not met the requirement for taking a meal break when working%' OR
            LOWER(validationmessages) LIKE 'the min/max break durations validation rule is incorrectly configured.%' OR
            LOWER(validationmessages) LIKE 'break should not exceed%' OR
            LOWER(validationmessages) LIKE 'break should be at least%' OR
            LOWER(validationmessages) LIKE 'the total hours for the week should be equal to%' OR
            LOWER(validationmessages) LIKE 'the total hours for the week should be less than or equal to%' OR
            LOWER(validationmessages) LIKE 'is going to terminate today%' OR
            LOWER(validationmessages) LIKE 'will be terminated in the next%' OR
            LOWER(validationmessages) LIKE 'you have time without a wbs/so, to review and change your entries if they should be recorded against a wbs/so.%' OR
            LOWER(validationmessages) LIKE "please review your timesheet for any time entries without a wbs/so to make sure a wbs/so is not expected. if a wbs/so is not required, you can ignore this warning message and submit your timesheet as normal. contact your project manager (wbs owner) or direct manager if you can't select a wbs/so that should be associated with your time entry%" OR
            LOWER(validationmessages) LIKE '%the entry for the day should not exceed a maximum%' OR
            LOWER(validationmessages) LIKE '%the total hours for the week should be greater than or equal to%' OR
            LOWER(validationmessages) LIKE '%ia perner id is not available. please contact the wbs owner.%' OR
            LOWER(validationmessages) LIKE '%perner id (user alternate id) is not available. please contact the wbs owner.%' OR
            LOWER(validationmessages) LIKE '%you have not met the requirement for taking a meal break when working%' OR
            LOWER(validationmessages) LIKE '%the min/max break durations validation rule is incorrectly configured.%' OR
            LOWER(validationmessages) LIKE '%break should not exceed%' OR
            LOWER(validationmessages) LIKE '%break should be at least%' OR
            LOWER(validationmessages) LIKE '%the total hours for the week should be equal to%' OR
            LOWER(validationmessages) LIKE '%the total hours for the week should be less than or equal to%' OR
            LOWER(validationmessages) LIKE '%is going to terminate today%' OR
            LOWER(validationmessages) LIKE '%will be terminated in the next%' OR
            LOWER(validationmessages) LIKE '%you have time without a wbs/so, to review and change your entries if they should be recorded against a wbs/so.%' OR
            LOWER(validationmessages) LIKE "%please review your timesheet for any time entries without a wbs/so to make sure a wbs/so is not expected. if a wbs/so is not required, you can ignore this warning message and submit your timesheet as normal. contact your project manager (wbs owner) or direct manager if you can't select a wbs/so that should be associated with your time entry%" OR
            LOWER(validationmessages) LIKE '%the entry for the day should not exceed a maximum' OR
            LOWER(validationmessages) LIKE '%the total hours for the week should be greater than or equal to' OR
            LOWER(validationmessages) LIKE '%ia perner id is not available. please contact the wbs owner.' OR
            LOWER(validationmessages) LIKE '%perner id (user alternate id) is not available. please contact the wbs owner.' OR
            LOWER(validationmessages) LIKE '%you have not met the requirement for taking a meal break when working' OR
            LOWER(validationmessages) LIKE '%the min/max break durations validation rule is incorrectly configured.' OR
            LOWER(validationmessages) LIKE '%break should not exceed' OR
            LOWER(validationmessages) LIKE '%break should be at least' OR
            LOWER(validationmessages) LIKE '%the total hours for the week should be equal to' OR
            LOWER(validationmessages) LIKE '%the total hours for the week should be less than or equal to' OR
            LOWER(validationmessages) LIKE '%is going to terminate today' OR
            LOWER(validationmessages) LIKE '%will be terminated in the next' OR
            LOWER(validationmessages) LIKE '%you have time without a wbs/so, to review and change your entries if they should be recorded against a wbs/so.' OR
            LOWER(validationmessages) LIKE "%please review your timesheet for any time entries without a wbs/so to make sure a wbs/so is not expected. if a wbs/so is not required, you can ignore this warning message and submit your timesheet as normal. contact your project manager (wbs owner) or direct manager if you can't select a wbs/so that should be associated with your time entry")
            """,
    )

    has_valid_data = rail.IfOperator(
        task_id='has_valid_data',
        test='{{ result("query_timesheet_report_collection", "length") > 0 }}',
        yes_task="process_time_entry_submission",
        no_task='end'
    )

    process_time_entry_submission = rail.TriggerDagRunForEachItemOperator(
        task_id='process_time_entry_submission',
        retries=0,
        items=lambda: rail.result("query_timesheet_report_collection"),
        trigger_dag_id=f'dxctechnology_time_entry_submission_child_{config.instance}',
        execution_timeout=timedelta(days=config.execution_timeout_days),
        conf={
            'Timesheeturi': "{{ item.timesheeturi}}",
            'User': "{{ item.username}}",
            'Period': "{{ item.timesheetperiod}}",
                'Username': "{{ item.username}}",
                'Employeeid': "{{ item.employeeid}}",
                'Timesheetstartdate': "{{ item.timesheetstartdate}}",
                'Timesheetenddate': "{{ item.timesheetenddate}}",
                'Useruri': "{{ item.useruri}}",
                'Country': config.country
        }
    )

    wait_for_process_time_entry_submission = rail.WaitForDagRunsSensor(
        task_id='wait_for_process_time_entry_submission',
        execution_timeout=timedelta(days=config.execution_timeout_days),
        dag_runs='{{ result("process_time_entry_submission") }}',
    )

    generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

    get_errored_logs = rail.FilterLogEntriesOperator(
        task_id='get_errored_logs',
        properties={'status': 'Error'}
    )

    get_exception_logs = rail.FilterLogEntriesOperator(
        task_id='get_exception_logs',
        properties={'status': 'Exception'}
    )

    get_success_logs = rail.FilterLogEntriesOperator(
        task_id='get_success_logs',
        properties={'status': 'Success'}
    )

    compose_csv = rail.WriteCSVFileOperator(
        task_id='compose_csv',
        source="{{ get_master_log() }}",
        header=[
            'employeeid',
            'username',
                'timesheetperiod',
                'datetime',
                'status',
                'remarks',
                'country',
                'ecid'],
        row=[
            '{{ item.properties | attr_or_default("employeeid", "") }}',
            '{{ item.properties | attr_or_default("username", "") }}',
            '{{ item.properties | attr_or_default("timesheetperiod", "") }}',
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            '{{ item.properties | attr_or_default("status", "") }}',
            '{{ item.properties | attr_or_default("details", "") }}',
            '{{ item.properties | attr_or_default("country", "") }}',
            '{{ item.ecid }}',
        ],
        footer=[
            # pylint: disable=line-too-long
            'Number of Records Processed Successfully: {{ result("query_timesheet_report_collection", key="length") - result("get_errored_logs", key="length") - result("get_exception_logs", key="length") }}',
            'Number of Records with Error: {{ result("get_errored_logs", key="length") }}',
            'Number of Records with Exception: {{ result("get_exception_logs", key="length") }}',
        ],
    )

    upload_file_to_sftp = rail.SFTPUploadFileOperator(
        task_id='upload_file_to_sftp',
        sftp_conn_id=config.sftp_conn_id,
        remote_filepath=config.log_filepath +
        'TimeEntry_auto_submission_' +
        config.country+'_logs_'+'{{ dag_run_ecid()}}.csv',
        content="{{ result('compose_csv') }}",
    )

    send_import_complete_email = rail.EmailOperator(
        task_id='send_import_complete_email',
        to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
        subject='{{ get_company_key() + " |  Automatic TimeEntry Submission -  " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " for " + params.country + " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
        html_content='templates/email/import_complete.html',
        params={
            'log_filepath': config.log_filepath,
                'country': config.country
        }
    )

    log_to_sumo = rail.DagRunLogToSumoOperator(
        task_id='log_to_sumo',
        sumo_conn_id='sumologic-dagrunlogger',
        trigger_rule='all_done',
        extra_info={
            'No_of_not_submitted_timesheet': '{{ result("query_timesheet_report_collection","length") }}',
            'Employee Type': 'ALL',
            'No of timesheet to be processed': '{{ result("query_timesheet_report_collection","length") }}',
                'Start Date - End Date': custom_methods.get_dates(config.pacific_timezone)
        }
    )

    end = rail.EmptyOperator(
        task_id='end'
    )

    is_valid_schedule >> rail.Label(
        "Yes") >> dxc_timeentry_auto_submission_location_mapper_search_entries_2

    is_valid_schedule >> rail.Label(
        "No") >> delete_this_dagrun

    dxc_timeentry_auto_submission_location_mapper_search_entries_2
    dxc_timeentry_auto_submission_location_mapper_search_entries_2 >> accumulate_list_items_3
    accumulate_list_items_3 >> get_report_details
    get_report_details >> get_enabled_locations >> \
        load_users_data_from_report >> has_load_users_data_from_report_data

    has_load_users_data_from_report_data >> rail.Label(
        "Yes") >> check_batch_error >> is_error_present_in_batch

    has_load_users_data_from_report_data >> rail.Label(
        "No") >> end

    is_error_present_in_batch >> rail.Label(
        "Yes") >> fail_with_error_log

    is_error_present_in_batch >> rail.Label(
        "No") >> load_users_data_from_report_payload_to_csv >> timesheet_report_collection >> \
        query_collection_data >> query_timesheet_report_collection >> has_valid_data

    has_valid_data >> rail.Label(
        "Yes") >> process_time_entry_submission >> wait_for_process_time_entry_submission

    wait_for_process_time_entry_submission >> generate_output_log \
        >> [get_errored_logs, get_exception_logs, get_success_logs] \
        >> compose_csv >> upload_file_to_sftp >> send_import_complete_email >> end

    has_valid_data >> rail.Label(
        "No") >> end >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
