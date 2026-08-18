from datetime import timedelta
from pendulum import now, datetime as dt
from functools import lru_cache
from collections import defaultdict
from airflow.models import Variable
from wipro.annual_leave_balance_transfer_france.utils import request_payload, response_filters
from wipro.annual_leave_balance_transfer_france.task.get_default_policy_schedules import get_default_policyschedules_task_group
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'WIPRO | Annual leave Balance Transfer | Annual Leaves To Carried Over {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_annual_leave,
        max_active_runs=config.max_active_runs_master,
        
    ) as dag:

        DATE_IN_REQUIRED_TIMEZONE = now(tz=config.time_zone)

        def can_trigger_run(dag_run):
            can_run = Variable.get(config.can_force_run, default_var='false') == 'true'
            if not can_run:
                can_run = bool(dag_run.conf.get('skip_rundate_validation', False) or \
                (DATE_IN_REQUIRED_TIMEZONE.strftime("%Y/%m/%d") == DATE_IN_REQUIRED_TIMEZONE.strftime("%Y") + "/06/01"))
            return can_run

        force_run_or_june_1st = rail.IfOperator(
            task_id='force_run_or_june_1st',
            test=can_trigger_run,
            yes_task='dag_run_log_time_info'
        )

        dag_run_log_time_info = rail.PythonOperator(
            task_id='dag_run_log_time_info',
            python_callable=lambda: {
                'current_date_time': DATE_IN_REQUIRED_TIMEZONE.strftime("%m-%d-%Y-%H-%M-%S"),
                'dag_run_date': DATE_IN_REQUIRED_TIMEZONE.strftime("%Y") + "/06/01",
                'report_run_date': str(int(DATE_IN_REQUIRED_TIMEZONE.strftime("%Y"))) + "/05/31",
                'log_filename': 'log_annual_leave_TO_balance_transfer_france_' + \
                    DATE_IN_REQUIRED_TIMEZONE.strftime("%Y%m%dT%H%M%S") + ".csv"
            }
        )        

        create_balance_transfer_logs = rail.CreateLogOperator(
            task_id='create_balance_transfer_logs'
        )

        get_required_timeoff_type_uris = rail.RepliconServiceOperator(
            task_id='get_required_timeoff_type_uris',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: response_filters.get_required_timeoff_type_uris_details(
                response, config)
        )
        
        process_get_default_policy_schedule_entry,  process_get_default_policy_schedule_exit= get_default_policyschedules_task_group(config)

        get_country_servicecenter_uri = rail.RepliconServiceOperator(
            task_id='get_country_servicecenter_uri',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.country, 'uri')
        )

        if_get_country_servicecenter_uri_not_found = rail.IfOperator(
            task_id='if_get_country_servicecenter_uri_not_found',
            test=lambda: not (rail.result(
                "get_country_servicecenter_uri")),
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

        run_report_timeoff_data = rail.run_report2(
            group_id="run_report_timeoff_data",
            report_params=request_payload.get_report_parameters,
            target='artifact',
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ (result('run_report_timeoff_data.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
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

        query_zero_balance_timeoff = rail.QueryCollectionOperator(
            task_id='query_zero_balance_timeoff',
            query="""SELECT * FROM report_data_collection WHERE timeoff_balance == '0.00'""",
            name='skipped_records'
        )

        has_zero_balance = rail.IfOperator(
            task_id='has_zero_balance',
            test=lambda: rail.result(
                "query_zero_balance_timeoff", "length") > 0,
            yes_task='log_skipped_records',
            no_task='query_distinct_loginname_records'
        )

        log_skipped_records = rail.WriteLogOperator(
            task_id='log_skipped_records',
            log="{{result('create_balance_transfer_logs')}}",
            items="{{result('query_zero_balance_timeoff')}}",
            message='na',
            severity='Exception',
            properties=lambda item: {
                "login_name": item['login_name'],
                "status": "Skipped",
                "details": f"Annual Leave Balance Transfer Not Processed as time off balance is 0 for the time off type - {item['timeoff_type']} as of date {rail.result('dag_run_log_time_info')['report_run_date']}"
            }
        )

        query_distinct_loginname_records = rail.QueryCollectionOperator(
            task_id='query_distinct_loginname_records',
            query="""SELECT distinct login_name FROM report_data_collection WHERE (timeoff_balance != '0.00' AND NULLIF(login_name, '') IS NOT NULL)""",
            name='distinct_login_names'
        )

        @lru_cache(maxsize=8)
        def get_all_timeoff_type_uris():
            return {**rail.result("get_required_timeoff_type_uris")['from'], **rail.result("get_required_timeoff_type_uris")['into']}

        trigger_dag_run_transfer_timeoff_balance = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_transfer_timeoff_balance',
            items="{{result('query_distinct_loginname_records')}}",
            trigger_dag_id=config.child_dag,
            conf=lambda item: {
                "login_name": item['login_name'],
                "timeoff_type_uris": rail.result("get_required_timeoff_type_uris"),
                "all_timeoff_type_uris": get_all_timeoff_type_uris(),
                "new_policy_effective_date": rail.result('dag_run_log_time_info')['dag_run_date'],
                "balance_transfer_log": rail.result("create_balance_transfer_logs"),
                "get_annual_leave_default_policy": rail.result("get_annual_leave_default_policy"),
                "get_annual_leave_carried_over_default_policy": rail.result("get_annual_leave_carried_over_default_policy"),
                "get_annual_leave_seniority_days_carried_over_default_policy": rail.result("get_annual_leave_seniority_days_carried_over_default_policy"),
                "get_annual_leave_rtt_carried_over_default_policy": rail.result("get_annual_leave_rtt_carried_over_default_policy"),
                "get_annual_leave_rtt_for_forfait_jours_carried_over_default_policy": rail.result("get_annual_leave_rtt_for_forfait_jours_carried_over_default_policy"),
            },
            parallel_count=config.parallel_transfer_dag_count,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id='compose_logs_csv',
            source="{{ result('create_balance_transfer_logs') }}",
            header=['Login Name', 'Status', 'Details', 'JobId'],
            row=lambda item: [
                item['properties']['login_name'],
                item['properties']['status'],
                item['properties']['details'],
                item['ecid']
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name='{{result("dag_run_log_time_info").log_filename }}',
            expires_in_seconds=7*24*60*60,
        )

        check_for_error_log = rail.FilterLogEntriesOperator(
            task_id='check_for_error_log',
            log="{{result('create_balance_transfer_logs')}}",
            severity='Error'
        )

        check_for_exception_log = rail.FilterLogEntriesOperator(
            task_id='check_for_exception_log',
            log="{{result('create_balance_transfer_logs')}}",
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
            subject='{{ get_company_key() + " | Annual leave balance transfer for France - " }} \
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

        force_run_or_june_1st >> rail.Label("Yes") >> dag_run_log_time_info >> \
        create_balance_transfer_logs >> get_required_timeoff_type_uris >> process_get_default_policy_schedule_entry
        process_get_default_policy_schedule_exit >> get_country_servicecenter_uri \
            >> if_get_country_servicecenter_uri_not_found

        if_get_country_servicecenter_uri_not_found >> rail.Label(
            "Yes") >> fail_country_service_centre_not_found
        if_get_country_servicecenter_uri_not_found >> rail.Label(
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

        process_report_data >> load_csv >> create_collection_from_report_data >> query_zero_balance_timeoff

        query_zero_balance_timeoff >> has_zero_balance

        has_zero_balance >> rail.Label(
            "Yes") >> log_skipped_records >> query_distinct_loginname_records
        has_zero_balance >> rail.Label(
            "No") >> query_distinct_loginname_records

        query_distinct_loginname_records >> trigger_dag_run_transfer_timeoff_balance >> compose_logs_csv \
         >> generate_download_link >> check_for_error_log >> check_for_exception_log >> send_import_complete_email >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
