from datetime import timedelta
import json
import itertools
from pendulum import now, datetime as dt
from ipipeline.timeoff_policy_custom_script.utils import python_callable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.daily_run_master,
        description=f'iPipeline | YEAR END POLICY LINE - CUSTOM SCRIPT - Daily Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_daily,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        DATE_IN_REQUIRED_TIMEZONE = now(tz=config.time_zone)

        dag_run_log_time_info = rail.PythonOperator(
            task_id='dag_run_log_time_info',
            python_callable=lambda: {
                'current_date_time': DATE_IN_REQUIRED_TIMEZONE.strftime("%m%d%YT%H%M%S"),
                'dag_run_date': DATE_IN_REQUIRED_TIMEZONE.strftime("%Y/%m/%d"),
                'report_run_date': str(int(DATE_IN_REQUIRED_TIMEZONE.strftime("%Y")) - 1) + "/12/31",
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.daily_timeoff_report
        )

        run_report_timeoff_data = rail.run_report2(
            group_id="run_report_timeoff_data",
            # report_params=python_callable.get_report_parameters,
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
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
            no_task='send_no_data_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Policy Line Add - No Data - {{ result("dag_run_log_time_info").current_date_time }}',
            html_content="templates/no_records_email.html"
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
            headers=['username', 'useruri', 'login_name', 'user_start_date', 'timeoff_type', 'timeoff_uri',
                    'fte', 'scheduled_hours'],
            delimiter=','
        )

        create_collection_from_report_data = rail.CreateCollectionOperator(
            task_id='create_collection_from_report_data',
            name='report_data_collection',
            source="{{result('load_csv')}}"
        )

        query_records_to_process = rail.QueryCollectionOperator(
            task_id='query_records_to_process',
            query="""SELECT * FROM report_data_collection
                WHERE substr(user_start_date, 1, 5) = strftime('%m/%d', datetime('now', 'utc'))""",
            name='records_to_process'
        )

        query_uniq_timeoff_uris = rail.QueryCollectionOperator(
            task_id='query_uniq_timeoff_uris',
            query="""SELECT distinct timeoff_uri FROM report_data_collection""",
            name='uniq_timeoff_uris'
        )

        get_default_policies = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_default_policies',
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType',
            items=lambda : rail.result('query_uniq_timeoff_uris'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data={
                "timeOffTypeUri": "{{ item.timeoff_uri }}"
            },
            data_handler=lambda response, item: {
                "timeOffUri": item['timeoff_uri'],
                "defaultPolicy": json.loads(json.dumps(
                    response, ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
            },
            target='artifact'
        )

        get_all_script_uris = rail.RepliconServiceOperator(
            task_id='get_all_script_uris',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            target='artifact'
        )

        trigger_policyline_update = rail.trigger_parallel_dagrun(
            task_id='trigger_policyline_update',
            items="{{result('query_records_to_process')}}",
            trigger_dag_id=config.add_policyline_child,
            conf=lambda item: {
                "login_name": item['login_name'],
                "timeoff_type": item['timeoff_type'],
                "timeoff_type_uri": item['timeoff_uri'],
                "effective_date_for_new_policyset": rail.result('dag_run_log_time_info')['dag_run_date'],
                "get_default_policy": python_callable.get_default_policyline(item['timeoff_uri']),
                "get_all_script_uris": rail.result('get_all_script_uris')
            },
            parallel_count=config.process_users_dagruns_count,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_trigger_policyline_update_dag_ids =rail.PythonOperator(
            task_id= 'get_trigger_policyline_update_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_policyline_update_{x+1}'), range(config.process_users_dagruns_count))))),
            show_return_value_in_logs= False
        )

        gather_policyline_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_policyline_logs',
            dag_runs='{{ result("get_trigger_policyline_update_dag_ids") }}',
            dagrun_task_id='create_details_logs',
            execution_timeout=timedelta(
                hours=config.execution_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf={
                'policylogs': "{{result('gather_policyline_logs')}}",
                'current_date_time': "{{result('dag_run_log_time_info').current_date_time }}",
                'log_filename': "{{get_company_key()}}_anniversary_policy_line_{{result('dag_run_log_time_info').current_date_time }}.csv",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        dag_run_log_time_info >> get_report_details

        get_report_details >> run_report_timeoff_data >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> send_no_data_email

        is_report_has_expected_columns >> rail.Label(
            "Yes") >> process_report_data
        is_report_has_expected_columns >> rail.Label(
            "No") >> fail_no_expected_columns

        process_report_data >> load_csv >> create_collection_from_report_data >> query_records_to_process >> \
        query_uniq_timeoff_uris >> get_default_policies >> get_all_script_uris

        get_all_script_uris >> trigger_policyline_update >> get_trigger_policyline_update_dag_ids \
            >> gather_policyline_logs >> process_log_generation >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
