
from datetime import timedelta, datetime
import itertools
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'mpmq_disable_users_momentive_disable_users_daily_run_master_{config.instance}',
        description=f'Momentive_Disable Users Daily run_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_todaysdate_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_todaysdate_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_todaysdate_3 = rail.PythonOperator(
            task_id='log_todaysdate_3',
            python_callable=lambda:  datetime.utcnow().strftime("%Y-%m-%dT%H-%M")
        )

        get_enabled_employees_5 = rail.RepliconServiceOperator(
            task_id='get_enabled_employees_5',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:end-date",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        invoke_custom_ruby_code_8 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_8',
            python_callable=lambda: {"userlistinput": list(map(lambda item: {
                "user": item['cells'][1]['textValue'],
                "uri": item['cells'][1]['uri'],
                "enddate": item['cells'][0]['textValue'],
                "daydiff": (datetime.utcnow() - datetime(**item['cells'][0]['dateValue'])).days if item['cells'][0]['textValue'] else -10,
                "loginname": item['cells'][2]['textValue']
            }, rail.result('get_enabled_employees_5')['rows']))
            }
        )

        create_list_9 = rail.CreateCollectionOperator(
            task_id='create_list_9',
            source="{{ result('invoke_custom_ruby_code_8').userlistinput | to_json }}",
            name="enableduserlist",
        )

        query_list_getalluserswithenddateastodayorinpast_10 = rail.QueryCollectionOperator(
            task_id='query_list_getalluserswithenddateastodayorinpast_10',
            query="""SELECT * FROM  enableduserlist WHERE  NULLIF(enableduserlist.enddate,'') IS NOT NULL AND CAST(enableduserlist.daydiff as DECIMAL) > -1""",
        )

        if_query_list_getalluserswithenddateastodayorinpast_10_has_data = rail.IfOperator(
            task_id='if_query_list_getalluserswithenddateastodayorinpast_10',
            test=lambda: rail.result(
                'query_list_getalluserswithenddateastodayorinpast_10', 'length') > 0,
            yes_task='trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12',
            no_task='send_mail_sendemailcompletednorecordsfound_32'
        )

        trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12',
            retries=0,
            items="{{ result('query_list_getalluserswithenddateastodayorinpast_10') }}",
            trigger_dag_id=f'mpmq_disable_users_momentive_child_workflow_to_disable_user_daily_run_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "useruri": "{{ item.uri }}",
                "username": "{{ item.user }}",
                "loginname": "{{ item.loginname }}",
                "terminationdate": "{{ item.enddate }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12") }}',
            dagrun_task_id='create_log',
            flatten=True,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs'))))))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        get_logged_exceptions = rail.PythonOperator(
            task_id='get_logged_exceptions',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Exception')
        )

        get_logged_success = rail.PythonOperator(
            task_id='get_logged_success',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Success')
        )

        create_csv_lines_16 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_16',
            source="{{ result('format_logs') | to_json }}",
            header=['jobid',
                    'Child job id',
                    'user name',
                    'login name',
                    'useruri',
                    'status',
                    'details'],
            row=[
                "{{ dag_run_ecid() }}",
                "{{ item.ecid }}",
                "{{ item.properties.user_name }}",
                "{{ item.properties.login_name }}",
                "{{ item.properties.useruri }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}"
            ],
        )

        log_checkforerrors_17 = rail.PythonOperator(
            task_id='log_checkforerrors_17',
            python_callable=lambda:  bool(rail.result('get_logged_errors'))
        )

        log_logfilename_18 = rail.PythonOperator(
            task_id='log_logfilename_18',
            python_callable=lambda: rail.render_template(
                "disablelogs_{{ result('log_todaysdate_3') }}.csv")
        )

        get_signed_download_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id='get_signed_download_url',
            artifact_name='''{{ result('create_csv_lines_16') }}''',
            output_file_name="{{ result('log_logfilename_18') }}",
            expires_in_seconds=7*24*60*60,
        )

        if_log_checkforerrors_17_present_24 = rail.IfOperator(
            task_id='if_log_checkforerrors_17_present_24',
            test='''{{ result('log_checkforerrors_17') | is_truthy }}''',
            yes_task="send_mail_with_cshare_sendemailcompletedwitherrors_25",
            no_task="send_mail_with_cshare_sendemailcompletedsuccessfully_27",
        )

        send_mail_with_cshare_sendemailcompletedwitherrors_25 = rail.EmailOperator(
            task_id='send_mail_with_cshare_sendemailcompletedwitherrors_25',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} | Disable User run completed with errors - {{ current_time() }} ''',
            html_content='''<strong>This is an automated mail, please don't reply</strong><br />
<br />
Hello, <br />
<br />
The disable user daily run is completed with errors at {{ result('log_todaysdate_3') }}. Please find the link below to the logs for reference.
<p><em><a href="{{ result('get_signed_download_url') }}">Download Logs</a></em>
<br />
    <span style="font-size: small;">The download link is valid for 30 days.</span><br>
</p>
<p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_with_cshare_sendemailcompletedsuccessfully_27 = rail.EmailOperator(
            task_id='send_mail_with_cshare_sendemailcompletedsuccessfully_27',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Disable User run completed successfully - {{ current_time() }} ''',
            html_content='''<strong>This is an automated mail, please don't reply</strong><br />
<br />
Hello, <br />
<br />
The disable user daily run is completed successfully at {{ result('log_todaysdate_3') }}. Please find the link below to the logs for reference.
<p><em><a href="{{ result('get_signed_download_url') }}">Download Logs</a></em>
<br />
    <span style="font-size: small;">The download link is valid for 30 days.</span><br>
</p>
<p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_sendemailcompletednorecordsfound_32 = rail.EmailOperator(
            task_id='send_mail_sendemailcompletednorecordsfound_32',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Disable User run completed - No records found - {{ current_time() }} ''',
            html_content='''<strong>This is an automated mail, please don't reply</strong><br />
<br />
Hello, <br />
<br />
The disable user daily run was completed at {{ result('log_todaysdate_3') }}. There were no records found matching the criteria to disable the logins. <br/>
<p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_todaysdate_3
        log_todaysdate_3 >> get_enabled_employees_5 >> invoke_custom_ruby_code_8 >> create_list_9 >> query_list_getalluserswithenddateastodayorinpast_10 >> if_query_list_getalluserswithenddateastodayorinpast_10_has_data
        if_query_list_getalluserswithenddateastodayorinpast_10_has_data >> rail.Label(
            'yes') >> trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12 >> wait_for_completion_trigger_dag_run_live_momentive_child_workflow_to_disable_user_daily_runasync_12 >> gather_child_logs >> format_logs >> get_logged_errors >> get_logged_exceptions >> get_logged_success >> create_csv_lines_16 >> log_checkforerrors_17 >> log_logfilename_18 >> get_signed_download_url >> if_log_checkforerrors_17_present_24
        if_log_checkforerrors_17_present_24 >> rail.Label(
            'Yes') >> send_mail_with_cshare_sendemailcompletedwitherrors_25 >> finish
        if_log_checkforerrors_17_present_24 >> rail.Label(
            'No') >> send_mail_with_cshare_sendemailcompletedsuccessfully_27 >> finish
        if_query_list_getalluserswithenddateastodayorinpast_10_has_data >> rail.Label(
            'No') >> send_mail_sendemailcompletednorecordsfound_32 >> finish
        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
