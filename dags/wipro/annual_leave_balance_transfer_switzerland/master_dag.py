from datetime import timedelta
from pendulum import now, datetime as dt
from wipro.annual_leave_balance_transfer_switzerland.utils import python_callable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.annual_leaves_balance_transfer_year_end_dag_id,
        description=f'WIPRO | Annual leave Balance Transfer | Annual Leaves To Additional Leaves {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_annual_leave,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        DATE_IN_REQUIRED_TIMEZONE = now(tz=config.time_zone)

        def can_trigger_run(dag_run):
            return bool(dag_run.conf.get('skip_rundate_validation', False) or \
                (DATE_IN_REQUIRED_TIMEZONE.strftime("%Y/%m/%d") == DATE_IN_REQUIRED_TIMEZONE.strftime("%Y") + "/01/01"))

        can_run = rail.IfOperator(
            task_id='can_run',
            test=can_trigger_run,
            yes_task='dag_run_log_time_info'
        )

        dag_run_log_time_info = rail.PythonOperator(
            task_id='dag_run_log_time_info',
            python_callable=lambda: {
                'current_date_time': DATE_IN_REQUIRED_TIMEZONE.strftime("%m%d%YT%H%M%S"),
                'dag_run_date': DATE_IN_REQUIRED_TIMEZONE.strftime("%Y") + "/01/01",
                'next_effective_date': DATE_IN_REQUIRED_TIMEZONE.strftime("%Y") + "/01/02",
                'report_run_date': str(int(DATE_IN_REQUIRED_TIMEZONE.strftime("%Y")) - 1) + "/12/31",
            }
        )

        create_timeoff_balance_transfer_logs = rail.CreateLogOperator(
            task_id='create_timeoff_balance_transfer_logs'
        )

        log_get_required_timeoff_type_uris = rail.RepliconServiceOperator(
            task_id='log_get_required_timeoff_type_uris',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: python_callable.get_required_timeoff_type_uris(config,response)
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

        run_report_timeoff_data = rail.run_report2(
            group_id="run_report_timeoff_data",
            report_params=python_callable.get_report_parameters,
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
                     'user_start_date', 'employee_id', 'login_name', 'country', 'fte', 'onsite_direct_recruit'],
            delimiter=','
        )

        create_collection_from_report_data = rail.CreateCollectionOperator(
            task_id='create_collection_from_report_data',
            name='report_data_collection',
            source="{{result('load_csv')}}"
        )

        get_all_from_timeoffs = rail.PythonOperator(
            task_id = 'get_all_from_timeoffs',
            python_callable=python_callable.get_all_from_timeoff_types
        )

        query_from_records = rail.QueryCollectionOperator(
            task_id='query_from_records',
            query="""SELECT * FROM report_data_collection WHERE timeoff_type in ({{result('get_all_from_timeoffs').from}})""",
            name='from_records'
        )

        query_into_records = rail.QueryCollectionOperator(
            task_id='query_into_records',
            query="""SELECT * FROM report_data_collection WHERE timeoff_type in ({{result('get_all_from_timeoffs').into}})""",
            name='into_records'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query="""SELECT * FROM from_records WHERE timeoff_balance < '0.00'""",
            name='invalid_records'
        )

        if_invalid_records_greater_than_0 = rail.IfOperator(
            task_id='if_invalid_records_greater_than_0',
            test=lambda: rail.result("query_invalid_records", "length") > 0,
            yes_task='log_invalid_records',
            no_task='query_valid_records'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{result('create_timeoff_balance_transfer_logs')}}",
            items="{{result('query_invalid_records')}}",
            message='na',
            severity=lambda item: 'Exception' if item['login_name'] else 'Error',
            properties= lambda item: {
                'jobid': "{{dag_run_ecid()}}",
                "login_name": item['login_name'] if item['login_name'] else '',
                "status": "Skipped" if item['login_name'] else "Error",
                "details": "Annual Leave Balance Transfer Not Processed as Login Name is not present in record" if not item['login_name'] else \
                    "Annual Leave Balance Transfer Not Processed as time off balance is 0 or less for the time off type- {} as of date {}".format\
                        (item['timeoff_type'], rail.result('dag_run_log_time_info')['report_run_date'])
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT distinct login_name FROM from_records WHERE timeoff_balance >= '0.00'""",
            name='valid_records_to_process'
        )

        get_default_policy = rail.RepliconServiceOperator(
            task_id='get_default_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_get_required_timeoff_type_uris').into.timeoff_annual_leave_additional_uri }}"
            },
            target='artifact'
        )

        trigger_dag_run_transfer_timeoff_balance = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_transfer_timeoff_balance',
            items="{{result('query_valid_records')}}",
            trigger_dag_id=config.child_workflow_to_transfer_timeoff_balance_dag_id,
            conf=lambda item: {
                'parentjobid': rail.render_template("{{dag_run_ecid()}}"),
                "login_name": item['login_name'],
                "timeoff_type_uri_for_transferring_balance_into": rail.result("log_get_required_timeoff_type_uris")['into']['timeoff_annual_leave_additional_uri'],
                'timeoff_type_name_for_transferring_balance_into': config.ANNUAL_LEAVE_ADDITIONAL,
                "effective_date_for_new_policyset": rail.result('dag_run_log_time_info')['dag_run_date'],
                "next_effective_date": rail.result('dag_run_log_time_info')['next_effective_date'],
                "get_default_policy": rail.result('get_default_policy'),
                "user_log": rail.result("create_timeoff_balance_transfer_logs")
            },
            parallel_count=config.process_users_for_timeoff_balance_transfer_parallel_dagruns_count,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id='compose_logs_csv',
            source="{{ result('create_timeoff_balance_transfer_logs') }}",
            header=['jobid', 'login_name', 'status', 'details'],
            row=lambda item: [
                item['properties']['jobid'],
                item['properties']['login_name'],
                item['properties']['status'],
                item['properties']['details'],
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name='{{get_company_key()}}_switzerland_annual_leave_balance_transfer_log_{{result("dag_run_log_time_info").current_date_time }}.csv',
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
            bcc="{%- if result('check_for_error_log', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Switzerland Annual leave balance transfer - " }} \
                {%- if result("check_for_error_log", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("check_for_exception_log", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + result("dag_run_log_time_info").current_date_time  }}',
            html_content="templates/transfer_complete_mail.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run >> rail.Label("Yes") >> dag_run_log_time_info

        dag_run_log_time_info >> create_timeoff_balance_transfer_logs >> log_get_required_timeoff_type_uris >> get_required_country_service_center_uri \
            >> if_get_required_country_service_center_uri_not_found

        if_get_required_country_service_center_uri_not_found >> rail.Label(
            "Yes") >> fail_country_service_centre_not_found
        if_get_required_country_service_center_uri_not_found >> rail.Label(
            "No") >> get_report_details

        get_report_details >> run_report_timeoff_data >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> fail_with_no_data_in_report

        is_report_has_expected_columns >> rail.Label(
            "Yes") >> process_report_data
        is_report_has_expected_columns >> rail.Label(
            "No") >> fail_no_expected_columns

        process_report_data >> load_csv >> create_collection_from_report_data >> get_all_from_timeoffs >> \
        query_from_records >> query_into_records >> query_invalid_records >> if_invalid_records_greater_than_0

        if_invalid_records_greater_than_0 >> rail.Label(
            "Yes") >> log_invalid_records >> query_valid_records
        if_invalid_records_greater_than_0 >> rail.Label(
            "No") >> query_valid_records

        query_valid_records >> get_default_policy >> trigger_dag_run_transfer_timeoff_balance >> compose_logs_csv \
            >> generate_download_link >> check_for_error_log >> check_for_exception_log >> send_import_complete_email >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
