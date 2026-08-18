
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_report_export_custom_supervisor_report_master_{config.instance}',
        description=f'Terracon_Custom Supervisor Report {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        webhook_conf=[rail.WebhookConf(
            bearer_token_var=f'terraconconsultants_custom_supervisor_report_webook_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details=rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data=lambda dag_run: {
                "userUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user:" + dag_run.conf['webhook']['data']['userid']
            }
        )

        if_emailaddress_not_present=rail.IfOperator(
            task_id='if_emailaddress_not_present',
            test='''{{result('get_user_details').emailAddress | is_falsy }}''',
            yes_task="finish",
            no_task="if_startdate_or_enddate_not_present",
        )

        if_startdate_or_enddate_not_present=rail.IfOperator(
            task_id='if_startdate_or_enddate_not_present',
            test=lambda dag_run: bool( (len((dag_run.conf['webhook']['data']['date']).split('-')) != 2) or
                                    ('' in (dag_run.conf['webhook']['data']['date']).split('-')) or
                                    ('null' in (dag_run.conf['webhook']['data']['date']).split('-'))
                                ),
            yes_task="send_mail_start_or_enddate_notselected",
            no_task="get_user_report_details",
        )

        send_mail_start_or_enddate_notselected=rail.EmailOperator(
            task_id='send_mail_start_or_enddate_notselected',
            to="{{result('get_user_details').emailAddress }}",
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Supervisor summary report extract - {{ current_time("%m/%d/%YT%H:%M:%S") }}',
            html_content= '''templates/startdate_or_enddate_not_selected_mail.html''',
            params=None,
        )

        get_user_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_report
        )

        get_timesheet_data_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_data_report_details',
            report_name=config.timesheet_data_report
        )

        get_paycode_data_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_paycode_data_report_details',
            report_name=config.paycode_data_report
        )

        if_uri_for_any_report_not_present=rail.IfOperator(
            task_id='if_uri_for_any_report_not_present',
            test=lambda: not bool(rail.result('get_user_report_details') and rail.result('get_user_report_details')['uri']
                            and rail.result('get_timesheet_data_report_details') and rail.result('get_timesheet_data_report_details')['uri']
                            and rail.result('get_paycode_data_report_details') and rail.result('get_paycode_data_report_details')['uri']),
            yes_task="fail_dag_with_error",
            no_task="get_filter_uris_for_timesheetdata_report",
        )

        def get_error_message():
            return ("" if rail.result('get_user_report_details') else "User Report - Custom is not present in Replicon") + ',' + ("" if
                rail.result('get_timesheet_data_report_details') else "Custom Base Report - Timesheet Data is not present in Replicon") + ',' + ("" if
                rail.result('get_paycode_data_report_details') else "Custom Base Report - Pay code Data is not present in Replicon")

        fail_dag_with_error=rail.FailOperator(
            task_id='fail_dag_with_error',
            message=get_error_message
        )

        get_filter_uris_for_timesheetdata_report=rail.PythonOperator(
            task_id='get_filter_uris_for_timesheetdata_report',
            python_callable= lambda:  {
                'userfilter': rail.find_first_by_attr_and_get_attr(rail.result('get_timesheet_data_report_details')['filterConfiguration']['enabledFilters'],
                                'displayText','UserFilter','uri',''),
                'entrydatefilter': rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_timesheet_data_report_details')['filterConfiguration']['enabledFilters'],'displayText','EntryDateFilter','uri','')
            }
        )

        get_filter_uris_for_paycodedata_report=rail.PythonOperator(
            task_id='get_filter_uris_for_paycodedata_report',
            python_callable= lambda: {
                'userfilter': rail.find_first_by_attr_and_get_attr(rail.result('get_paycode_data_report_details')['filterConfiguration']['enabledFilters'],
                                'displayText','UserFilter','uri',''),
                'entrydatefilter': rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_paycode_data_report_details')['filterConfiguration']['enabledFilters'],'displayText','EntryDateFilter','uri','')
            }
        )

        run_user_report = rail.run_report2(
            group_id='run_user_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_user_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_error_in_user_report=rail.IfOperator(
            task_id='if_error_in_user_report',
            #pylint: disable = line-too-long
            test='''{{ (result('run_user_report.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy or result('run_user_report.get_report_result','has_data') | is_falsy}}''',
            yes_task="fail_dag_error_in_report",
            no_task="parse_csv_user_report_data",
        )

        def get_message_for_error():
            report_result_error = rail.load_json_artifact(rail.result('run_user_report.get_report_result'))['reportGenerationResults'][0]['error']
            return (report_result_error if report_result_error else "No Data in user report")

        fail_dag_error_in_report=rail.FailOperator(
            task_id='fail_dag_error_in_report',
            message=get_message_for_error
        )

        parse_csv_user_report_data=rail.LoadCSVFileOperator(
            task_id='parse_csv_user_report_data',
            document="{{(result('run_user_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}"
        )

        def get_next_reportees(user_uri,user_data):
            result = []
            for user in user_data:
                if user['supervisoruri'] == user_uri:
                    result.append({
                        'name': user['User Name'],
                        'supervisor': user['User Supervisor Name (Current)'],
                        'useruri': user['useruri'],
                        'supervisoruri': user['supervisoruri'],
                    })
                    result.extend(get_next_reportees(user['useruri'],user_data))
            return result

        def get_direct_indirect_reportees():
            user_data = rail.load_all_records(rail.result('parse_csv_user_report_data'))
            requestor_uri = rail.result('get_user_details')['uri']
            return get_next_reportees(requestor_uri,user_data)

        get_reportees=rail.PythonOperator(
            task_id='get_reportees',
            python_callable=get_direct_indirect_reportees
        )

        def get_filters(dag_run):
            return {
                'timesheetdatafilter': [
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_timesheetdata_report')['entrydatefilter'],
                        "value": null
                    },
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_timesheetdata_report')['entrydatefilter'],
                        "value": (datetime.strptime(dag_run.conf['webhook']['data']['date'].split('-')[0],'%m%d%Y')).strftime('%m/%d/%Y')
                    },
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_timesheetdata_report')['entrydatefilter'],
                        "value": (datetime.strptime(dag_run.conf['webhook']['data']['date'].split('-')[-1],'%m%d%Y')).strftime('%m/%d/%Y')
                    },
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_timesheetdata_report')['userfilter'],
                        "value": dag_run.conf['webhook']['data']['userid']
                    }
                ],
                'paycodedatafilter': [
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_paycodedata_report')['entrydatefilter'],
                        "value": null
                    },
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_paycodedata_report')['entrydatefilter'],
                        "value": (datetime.strptime(dag_run.conf['webhook']['data']['date'].split('-')[0],'%m%d%Y')).strftime('%m/%d/%Y')
                    },
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_paycodedata_report')['entrydatefilter'],
                        "value": (datetime.strptime(dag_run.conf['webhook']['data']['date'].split('-')[-1],'%m%d%Y')).strftime('%m/%d/%Y')
                    },
                    {
                        "reportFilterUri": rail.result('get_filter_uris_for_paycodedata_report')['userfilter'],
                        "value": dag_run.conf['webhook']['data']['userid']
                    }
                ]
            }

        create_filters_for_two_reports = rail.PythonOperator(
            task_id = 'create_filters_for_two_reports',
            python_callable= get_filters
        )

        if_reportees_present=rail.IfOperator(
            task_id='if_reportees_present',
            test=lambda: len(rail.result('get_reportees')) > 0,
            yes_task="add_more_user_filters",
            no_task="log_filter_for_timesheetdata_report",
        )

        def add_users_in_filter():
            reportees = rail.result('get_reportees')
            timesheetdatareporturi = rail.result('get_filter_uris_for_timesheetdata_report')['userfilter']
            paycodedatareporturi = rail.result('get_filter_uris_for_paycodedata_report')['userfilter']
            timesheetdatafilter = []
            paycodedatafilter = []
            for reportee in reportees:
                timesheetdatafilter.append({
                    "reportFilterUri": timesheetdatareporturi,
                    "value": reportee['useruri'].split(':')[-1]
                })
                paycodedatafilter.append({
                    "reportFilterUri": paycodedatareporturi,
                    "value": reportee['useruri'].split(':')[-1]
                })
            return{
                'timesheetdatafilter':timesheetdatafilter,
                'paycodedatafilter': paycodedatafilter
            }

        add_more_user_filters = rail.PythonOperator(
            task_id = 'add_more_user_filters',
            python_callable= add_users_in_filter
        )

        log_filter_for_timesheetdata_report=rail.PythonOperator(
            task_id='log_filter_for_timesheetdata_report',
            python_callable= lambda: rail.result('create_filters_for_two_reports')['timesheetdatafilter'] +
                                rail.result('add_more_user_filters')['timesheetdatafilter']
        )

        run_timesheetdata_report = rail.run_report2(
            group_id='run_timesheetdata_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_timesheet_data_report_details')['uri'],
                        "filterValues": rail.result('log_filter_for_timesheetdata_report'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        log_filter_for_paycodedata_report=rail.PythonOperator(
            task_id='log_filter_for_paycodedata_report',
            python_callable= lambda: rail.result('create_filters_for_two_reports')['paycodedatafilter'] +
                                rail.result('add_more_user_filters')['paycodedatafilter']
        )

        run_paycodedata_report = rail.run_report2(
            group_id='run_paycodedata_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_paycode_data_report_details')['uri'],
                        "filterValues": rail.result('log_filter_for_paycodedata_report'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        trigger_child_custom_supervisor_report_export=rail.TriggerDagRunOperator(
            task_id='trigger_child_custom_supervisor_report_export',
            retries=0,
            trigger_dag_id=f'terraconconsultants_report_export_custom_supervisor_report_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                "timesheetdatabasereportresult": rail.result('run_timesheetdata_report.get_report_result'),
                "payrollbasereportresult": rail.result('run_paycodedata_report.get_report_result'),
                "datefield": datetime.strptime(dag_run.conf['webhook']['data']['date'].split('-')[0],'%m%d%Y').strftime('%m/%d/%Y') + ' - ' +
                                datetime.strptime(dag_run.conf['webhook']['data']['date'].split('-')[-1],'%m%d%Y').strftime('%m/%d/%Y'),
                "userid": dag_run.conf['webhook']['data']['userid'],
                "username": rail.result('get_user_details')['firstName'] + " | " + rail.result('get_user_details')['lastName'],
                "emailid": rail.result('get_user_details')['emailAddress']
            }
        )

        wait_for_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_custom_supervisor_report_export") }}'
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_user_details
        get_user_details >> if_emailaddress_not_present
        if_emailaddress_not_present >> rail.Label('Yes')  >> finish
        if_emailaddress_not_present >> rail.Label('No') >> if_startdate_or_enddate_not_present
        if_startdate_or_enddate_not_present >> rail.Label('Yes') >> send_mail_start_or_enddate_notselected >> finish
        if_startdate_or_enddate_not_present >> rail.Label('No') >> get_user_report_details >> get_timesheet_data_report_details
        get_timesheet_data_report_details >> get_paycode_data_report_details >> if_uri_for_any_report_not_present
        if_uri_for_any_report_not_present >> rail.Label('Yes')  >> fail_dag_with_error >> finish
        if_uri_for_any_report_not_present >> rail.Label('No') >> get_filter_uris_for_timesheetdata_report
        get_filter_uris_for_timesheetdata_report >> get_filter_uris_for_paycodedata_report >> run_user_report >> if_error_in_user_report
        if_error_in_user_report >> rail.Label('Yes')  >> fail_dag_error_in_report >> finish
        if_error_in_user_report >> rail.Label('No') >> parse_csv_user_report_data >> get_reportees >> create_filters_for_two_reports
        create_filters_for_two_reports >> if_reportees_present
        if_reportees_present >> rail.Label(
            'Yes')  >> add_more_user_filters >> log_filter_for_timesheetdata_report
        if_reportees_present >> rail.Label(
            'No') >> log_filter_for_timesheetdata_report >> run_timesheetdata_report >> log_filter_for_paycodedata_report >> run_paycodedata_report
        run_paycodedata_report >> trigger_child_custom_supervisor_report_export >> wait_for_child_dag >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
