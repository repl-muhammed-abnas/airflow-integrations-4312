from datetime import timedelta, datetime as dat
from pendulum import now, datetime as dt
import rail

from crl.vacation_balance_carry_over_canada.utils import request_payload
from crl.vacation_balance_carry_over_canada.utils import response_filter

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='CRL - CANADA - Vacation Balance Carry Over MASTER',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.ViewDagRunScheduleOperator(task_id="view_dagrun_schedule")

        ## FOR ADHOC RUNS USE BELOW CONF FORMAT
        # { 
        #   "skip_rundate_validation": true,
        #   "report_run_date": "%Y-%m-%d"
        # }
        def can_trigger_run(dag_run):
            return bool(dag_run.conf.get('skip_rundate_validation', False) or \
                (now(tz=config.time_zone).strftime("%Y/%m/%d") == now(tz=config.time_zone).strftime("%Y") + "/01/01"))

        if_run_date_is_1st_jan = rail.IfOperator(
            task_id='if_run_date_is_1st_jan',
            test=can_trigger_run,
            yes_task='get_required_timeoff_type_uri'
        )

        get_required_timeoff_type_uri = rail.RepliconServiceOperator(
            task_id='get_required_timeoff_type_uri',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response:response_filter.get_required_timeoff_type_uris(response, config)
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.balance_carry_over_report
        )

        def get_report_parameters(dag_run):
            as_of_date_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
            'displayText', 'AsOfDateFilter', 'uri')
            return {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": [
                            {
                                "reportFilterUri": as_of_date_filter_uri,
                                "value": "Date"
                            },
                            {
                                "reportFilterUri": as_of_date_filter_uri,
                                "value": dat.strptime(dag_run.conf["report_run_date"], "%Y-%m-%d").strftime("%Y-%m-%d")
                                if bool(dag_run.conf.get('skip_rundate_validation')) else (now(tz=config.time_zone) - timedelta(days=1)).strftime("%Y-%m-%d")
                            },
                            {
                                "reportFilterUri": as_of_date_filter_uri,
                                "value": null
                            }
                        ],
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
            headers=['login_name','user_uri', 'timeoff_type','units','timeoff_balance','std_hrs'],
            delimiter=','
        )

        create_collection_from_report_data = rail.CreateCollectionOperator(
            task_id='create_collection_from_report_data',
            name='report_data_collection',
            source="{{result('load_csv')}}"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query="""SELECT * FROM report_data_collection WHERE NULLIF(login_name, '') IS NULL or NULLIF(std_hrs, '') IS NULL or std_hrs ='0.00'""",
            name='invalid_records'
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM report_data_collection WHERE NULLIF(login_name, '') IS NOT NULL and NULLIF(std_hrs, '') IS NOT NULL and std_hrs !='0.00'""",
            name='valid_records_to_process'
        )
        
        trigger_dag_run_transfer_timeoff_balance = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_transfer_timeoff_balance',
            items="{{result('query_valid_records')}}",
            trigger_dag_id=config.child_dagid,
            conf=lambda item, dag_run: {
                "login_name": item['login_name'],
                "user_uri": item['user_uri'],
                "timeoff_type_balance": item['timeoff_balance'],
                "timeoff_type_name_from_which_balance_is_picked": item['timeoff_type'],
                "timeoff_type_uri_for_transferring_balance_into": rail.result("get_required_timeoff_type_uri")['timeoff_uri_to_transfer_balance_into'],
                "balance_to_transfer": request_payload.get_balace_to_transfer(item, config),
                "effective_date_for_new_policyset": request_payload.get_effective_date_for_new_policyset(dag_run,False),
                "expiry_date_for_new_policyset": request_payload.get_effective_date_for_new_policyset(dag_run,True),
                "remove_historical_policies": dag_run.conf['remove_historical_policies'] if bool(dag_run.conf.get('skip_rundate_validation')) else "No",
            },
            parallel_count=config.trigger_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        send_transfer_complete_email = rail.EmailOperator(
            task_id='send_transfer_complete_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() + " | Annual Canada Vacation Balance Transfer Completed " }} \
                {{ " - " +  current_time_in_specified_tz("US/Eastern") }}',
            html_content="templates/transfer_complete_mail.html",
        )

        if_run_date_is_1st_jan >> rail.Label('Yes') >> get_required_timeoff_type_uri

        get_required_timeoff_type_uri >> get_report_details >> run_report_timeoff_data >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> fail_with_no_data_in_report

        is_report_has_expected_columns >> rail.Label(
            "Yes") >> process_report_data
        is_report_has_expected_columns >> rail.Label(
            "No") >> fail_no_expected_columns

        process_report_data >> load_csv >> create_collection_from_report_data >> query_invalid_records >> query_valid_records
        query_valid_records >> trigger_dag_run_transfer_timeoff_balance >> send_transfer_complete_email

    return dag


rail.for_each_instance(create_dag)
