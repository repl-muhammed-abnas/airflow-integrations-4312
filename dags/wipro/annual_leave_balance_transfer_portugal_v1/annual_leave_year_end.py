from datetime import timedelta
from pendulum import now, datetime as dt
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.annual_leaves_to_carried_over_dag_id,
        description=f'WIPRO | Annual leave Balance Transfer for Portugal| Annual Leaves To Carried Over {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_annual_leave,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        DATE_IN_REQUIRED_TIMEZONE = now(tz=config.time_zone)

        def can_trigger_run(dag_run):
            return bool(dag_run.conf.get('skip_rundate_validation', False) or
                        (DATE_IN_REQUIRED_TIMEZONE.strftime("%Y/%m/%d") == DATE_IN_REQUIRED_TIMEZONE.strftime("%Y") + "/01/01"))

        if_run_date_is_1st_jan = rail.IfOperator(
            task_id='if_run_date_is_1st_jan',
            test=can_trigger_run,
            yes_task='log_dag_run_report_run_and_probation_cutoff_date'
        )

        log_dag_run_report_run_and_probation_cutoff_date = rail.PythonOperator(
            task_id='log_dag_run_report_run_and_probation_cutoff_date',
            python_callable=lambda: {
                'current_date_time': DATE_IN_REQUIRED_TIMEZONE.strftime("%m-%d-%Y-%H-%M-%S"),
                'dag_run_date': DATE_IN_REQUIRED_TIMEZONE.strftime("%Y") + "/01/01",
                'report_run_date': str(int(DATE_IN_REQUIRED_TIMEZONE.strftime("%Y")) - 1) + "/12/31",
                'probation_cutoff_date': str(int(DATE_IN_REQUIRED_TIMEZONE.strftime("%Y"))-1) + "/07/01"
            }
        )

        create_timeoff_balance_transfer_logs = rail.CreateLogOperator(
            task_id='create_timeoff_balance_transfer_logs'
        )

        def get_required_timeoff_type_uris(response):
            required_timeoff_types = list(filter(lambda y: y['displayText'] == config.ANNUAL_LEAVE or
                                                 y['displayText'] == config.ANNUAL_LEAVE_TRAVELPORT or y['displayText'] == config.ANNUAL_LEAVE_CARRIED_OVER, response))

            return {
                'timeoff_uris_to_pick_balance_from': [item['uri'] for item in required_timeoff_types if item['displayText'] == config.ANNUAL_LEAVE or item['displayText'] == config.ANNUAL_LEAVE_TRAVELPORT],
                'timeoff_uri_to_transfer_balance_into': rail.find_first_by_attr_and_get_attr(required_timeoff_types, 'displayText', config.ANNUAL_LEAVE_CARRIED_OVER, 'uri')
            }

        log_get_required_timeoff_type_uris = rail.RepliconServiceOperator(
            task_id='log_get_required_timeoff_type_uris',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: get_required_timeoff_type_uris(
                response)
        )

        get_required_country_service_center_uri = rail.RepliconServiceOperator(
            task_id='get_required_country_service_center_uri',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.country, 'uri')
        )

        if_get_required_country_service_center_uri_not_found = rail.IfOperator(
            task_id='if_get_required_country_service_center_uri_not_found',
            test=lambda: not (rail.result(
                "get_required_country_service_center_uri")),
            yes_task='fail_country_service_center_not_found',
            no_task='get_report_details'
        )

        fail_country_service_centre_not_found = rail.FailOperator(
            task_id='fail_country_service_center_not_found',
            message="Required country/service center not found in replicon"
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.annual_leave_balance_report
        )

        def get_filter_uris(enabled_filters):
            return {
                'country_service_centre_filter_uri': rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'CurrentServiceCenterFilter', 'uri'),
                'timeoff_type_filter_uri': rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'TimeOffTypeFilter', 'uri'),
                'as_of_date_filter_uri': rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'AsOfDateFilter', 'uri'),
            }

        get_required_filters = rail.PythonOperator(
            task_id='get_required_filters',
            python_callable=lambda: get_filter_uris(rail.result('get_report_details')[
                                                    'filterConfiguration']['enabledFilters'])
        )

        def get_report_parameters():
            filter_values = []

            filter_values.append({
                "reportFilterUri": rail.result('get_required_filters')['as_of_date_filter_uri'],
                "value": "DateRange"
            })
            filter_values.append({
                "reportFilterUri": rail.result('get_required_filters')['as_of_date_filter_uri'],
                "value": rail.result('log_dag_run_report_run_and_probation_cutoff_date')['report_run_date']
            })
            filter_values.append({
                "reportFilterUri": rail.result('get_required_filters')['as_of_date_filter_uri'],
                "value": rail.result('log_dag_run_report_run_and_probation_cutoff_date')['report_run_date']
            })

            for uri in rail.result("log_get_required_timeoff_type_uris")['timeoff_uris_to_pick_balance_from']:
                filter_values.append({
                    "reportFilterUri": rail.result('get_required_filters')['timeoff_type_filter_uri'],
                    "value": uri.split(":")[-1]
                })

            filter_values.append({
                "reportFilterUri": rail.result('get_required_filters')['country_service_centre_filter_uri'],
                "value": rail.result("get_required_country_service_center_uri").split(":")[-1]
            })

            return {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": filter_values,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

        run_report_timeoff_data = rail.run_report2(
            group_id="run_report_timeoff_data",
            report_params=get_report_parameters,
            target='artifact',
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ (result('run_report_timeoff_data.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message=lambda: rail.result('run_report_timeoff_data.get_report_result')[
                'reportGenerationResults'][0]['error']
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_report_timeoff_data.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='fail_with_no_data_in_report'
        )

        fail_with_no_data_in_report = rail.FailOperator(
            task_id='fail_with_no_data_in_report',
            message="Report has no Data"
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            test="{{ (result('run_report_timeoff_data.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='process_report_data',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        process_report_data = rail.EmptyOperator(
            task_id='process_report_data'
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ (result('run_report_timeoff_data.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            headers=['username', 'timeoff_type', 'timeoff_balance',
                     'user_start_date', 'employee_id', 'login_name', 'country'],
            delimiter=','
        )

        create_collection_from_report_data = rail.CreateCollectionOperator(
            task_id='create_collection_from_report_data',
            name='report_data_collection',
            source="{{result('load_csv')}}"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query="""SELECT * FROM report_data_collection WHERE NULLIF(login_name, '') IS NULL""",
            name='invalid_records'
        )

        if_invalid_records_greater_than_0 = rail.IfOperator(
            task_id='if_invalid_records_greater_than_0',
            test=lambda: rail.result("query_invalid_records", "length") > 0,
            yes_task='log_invalid_records',
            no_task='query_records_where_timeoff_balance_is_0'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{result('create_timeoff_balance_transfer_logs')}}",
            items="{{result('query_invalid_records')}}",
            message='na',
            severity='Error',
            properties={
                'jobid': "{{dag_run_ecid()}}",
                "login_name": '',
                "status": "Error",
                "details": "Annual Leave Balance Transfer Not Processed as Login Name is not present in record"
            }
        )

        query_records_where_timeoff_balance_is_0 = rail.QueryCollectionOperator(
            task_id='query_records_where_timeoff_balance_is_0',
            query="""SELECT * FROM report_data_collection WHERE (timeoff_balance == '0.00' AND NULLIF(login_name, '') IS NOT NULL)""",
            name='skipped_records'
        )

        if_records_with_timeoff_balance_0_greater_than_0 = rail.IfOperator(
            task_id='if_records_with_timeoff_balance_0_greater_than_0',
            test=lambda: rail.result(
                "query_records_where_timeoff_balance_is_0", "length") > 0,
            yes_task='log_skipped_records',
            no_task='query_records_where_timeoff_balance_is_not_0'
        )

        log_skipped_records = rail.WriteLogOperator(
            task_id='log_skipped_records',
            log="{{result('create_timeoff_balance_transfer_logs')}}",
            items="{{result('query_records_where_timeoff_balance_is_0')}}",
            message='na',
            severity='Exception',
            properties=lambda item: {
                'jobid': rail.render_template("{{dag_run_ecid()}}"),
                "login_name": item['login_name'],
                "status": "Skipped",
                "details": f"Annual Leave Balance Transfer Not Processed as time off balance is 0 for the time off type - {item['timeoff_type']} as of date {rail.result('log_dag_run_report_run_and_probation_cutoff_date')['report_run_date']}"
            }
        )

        query_records_where_timeoff_balance_is_not_0 = rail.QueryCollectionOperator(
            task_id='query_records_where_timeoff_balance_is_not_0',
            query="""SELECT * FROM report_data_collection WHERE (timeoff_balance != '0.00' AND NULLIF(login_name, '') IS NOT NULL)""",
            name='valid_records_to_process'
        )

        trigger_dag_run_transfer_timeoff_balance = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_transfer_timeoff_balance',
            items="{{result('query_records_where_timeoff_balance_is_not_0')}}",
            trigger_dag_id=config.child_workflow_to_transfer_timeoff_balance_dag_id,
            conf=lambda item: {
                'parentjobid': rail.render_template("{{dag_run_ecid()}}"),
                "login_name": item['login_name'],
                "timeoff_type_name_from_which_balance_is_picked": item['timeoff_type'],
                "timeoff_type_name_for_transferring_balance": config.ANNUAL_LEAVE_CARRIED_OVER,
                "timeoff_type_uri_for_transferring_balance": rail.result("log_get_required_timeoff_type_uris")['timeoff_uri_to_transfer_balance_into'],
                "balance_to_transfer": item['timeoff_balance'],
                "user_start_date": item['user_start_date'],
                "probation_cutoff_date": rail.result('log_dag_run_report_run_and_probation_cutoff_date')['probation_cutoff_date'],
                "efective_date_for_new_policyset": rail.result('log_dag_run_report_run_and_probation_cutoff_date')['dag_run_date'],
                "user_log": rail.result("create_timeoff_balance_transfer_logs")
            },
            parallel_count=config.process_users_for_timeoff_balance_transfer_parallel_dagruns_count,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_log_entries = rail.FilterLogEntriesOperator(
            task_id='search_log_entries',
            log="{{result('create_timeoff_balance_transfer_logs')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id='compose_logs_csv',
            source="{{ result('search_log_entries') }}",
            header=['login_name', 'status', 'details', 'jobid'],
            row=lambda item: [
                item['properties']['login_name'],
                item['properties']['status'],
                item['properties']['details'],
                item['ecid']
            ],
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content='''{{ result('compose_logs_csv') }}''',
            remote_filepath=config.log_filepath +
            '''/annual_leave_TO_balance_transfer_log_{{result("log_dag_run_report_run_and_probation_cutoff_date").current_date_time }}.csv''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name='annual_leave_TO_balance_transfer_log_{{result("log_dag_run_report_run_and_probation_cutoff_date").current_date_time }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        check_for_error_log = rail.FilterLogEntriesOperator(
            task_id='check_for_error_log',
            log="{{result('create_timeoff_balance_transfer_logs')}}",
            severity='Error'
        )

        check_for_exception_log = rail.FilterLogEntriesOperator(
            task_id='check_for_exception_log',
            log="{{result('create_timeoff_balance_transfer_logs')}}",
            severity='Exception'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc = "{%- if result('check_for_error_log', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Annual leave balance transfer for Portugal - " }} \
                {%- if result("check_for_error_log", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("check_for_exception_log", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + result("log_dag_run_report_run_and_probation_cutoff_date").current_date_time  }}',
            html_content="templates/transfer_complete_mail.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        if_run_date_is_1st_jan >> rail.Label(
            "Yes") >> log_dag_run_report_run_and_probation_cutoff_date

        log_dag_run_report_run_and_probation_cutoff_date >> create_timeoff_balance_transfer_logs >> log_get_required_timeoff_type_uris >> get_required_country_service_center_uri \
            >> if_get_required_country_service_center_uri_not_found

        if_get_required_country_service_center_uri_not_found >> rail.Label(
            "Yes") >> fail_country_service_centre_not_found
        if_get_required_country_service_center_uri_not_found >> rail.Label(
            "No") >> get_report_details

        get_report_details >> get_required_filters >> run_report_timeoff_data >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> fail_with_no_data_in_report

        is_report_has_expected_columns >> rail.Label(
            "Yes") >> process_report_data
        is_report_has_expected_columns >> rail.Label(
            "No") >> fail_no_expected_columns

        process_report_data >> load_csv >> create_collection_from_report_data >> query_invalid_records >> if_invalid_records_greater_than_0

        if_invalid_records_greater_than_0 >> rail.Label(
            "Yes") >> log_invalid_records >> query_records_where_timeoff_balance_is_0
        if_invalid_records_greater_than_0 >> rail.Label(
            "No") >> query_records_where_timeoff_balance_is_0

        query_records_where_timeoff_balance_is_0 >> if_records_with_timeoff_balance_0_greater_than_0

        if_records_with_timeoff_balance_0_greater_than_0 >> rail.Label(
            "Yes") >> log_skipped_records >> query_records_where_timeoff_balance_is_not_0
        if_records_with_timeoff_balance_0_greater_than_0 >> rail.Label(
            "No") >> query_records_where_timeoff_balance_is_not_0

        query_records_where_timeoff_balance_is_not_0 >> trigger_dag_run_transfer_timeoff_balance >> search_log_entries >> compose_logs_csv \
            >> upload_logs_to_sftp >> generate_download_link >> check_for_error_log >> check_for_exception_log >> send_import_complete_email >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
