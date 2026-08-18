from datetime import timedelta
import itertools
from pendulum import datetime
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_supervisororg_group_assignment_master_{config.instance}',
        description=f'HorizonMedia_supervisororg_group_assignment {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2023, 1, 1, tz=config.schedule_time_zone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.base_report_name,
        )

        generate_report_group = rail.run_report2(
            group_id='run_user_list_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        if_generate_report_4_payload_not_has_valid_columns_5 = rail.IfOperator(
            task_id='if_generate_report_4_payload_not_starts_with_loginnameuserurisupervisoryorgcurrent_5',
            test='''{{ not result("run_user_list_report.get_report_result").reportGenerationResults[0].payload | starts_with('Login Name,UserUri,Supervisory ORG (Current)') }}''',
            yes_task="stop_6",
            no_task="load_user_report_csv_data",
        )

        stop_6 = rail.FailOperator(
            task_id='stop_6',
            message='''Base report columns doesn't match'''
        )

        load_user_report_csv_data = rail.LoadCSVFileOperator(
            task_id='load_user_report_csv_data',
            document='{{ result("run_user_list_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        create_userlist_collection = rail.CreateCollectionOperator(
            task_id='create_userlist_collection',
            source="{{ result('load_user_report_csv_data') }}",
            name='repliconbasereport',
            columns={'Login Name': 'loginname', 'UserUri': 'useruri',
                     'Supervisory ORG (Current)': 'supervisororggroup',
                     'User Supervisor Name (Current)': 'supervisorname',
                     'supervisoruri': 'supervisoruri'}
        )

        load_all_base_report_data = rail.PythonOperator(
            task_id='load_all_base_report_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('create_userlist_collection'))
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        query_list_basereportdata_10 = rail.QueryCollectionOperator(
            task_id='query_list_basereportdata_10',
            query="""SELECT * FROM  repliconbasereport""",
        )

        query_list_distinct_enabledsupervisorsfrom_replicon_11 = rail.QueryCollectionOperator(
            task_id='query_list_distinct_enabledsupervisorsfrom_replicon_11',
            query="""SELECT DISTINCT  repliconbasereport.supervisoruri,  repliconbasereport.supervisorname FROM  repliconbasereport WHERE NOT  repliconbasereport.supervisoruri  NOT LIKE '%_%'""",
        )

        get_all_department_groups_get_all_departments_13 = rail.RepliconServiceOperator(
            task_id='get_all_department_groups_get_all_departments_13',
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )

        parallel_count = 10
        trigger_dag_run_process_supervisor = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_process_supervisor',
            parallel_count=parallel_count,
            items="{{ result('query_list_distinct_enabledsupervisorsfrom_replicon_11') }}",
            trigger_dag_id=f'horizonmedia_supervisororg_group_process_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "department_groups": rail.result('get_all_department_groups_get_all_departments_13'),
                "supervisorname": item['supervisorname'],
                "supervisoruri": item['supervisoruri'],
            }
        )

        get_all_child_dag_runs = rail.PythonOperator(
            task_id='get_all_child_dag_runs',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_dag_run_process_supervisor_{x+1}'), range(parallel_count)))))
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("get_all_child_dag_runs") }}',
            dagrun_task_id='create_log',
            flatten=True,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs')+[rail.result('create_log')])))))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        create_csv_lines_29 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_29',
            source="{{ result('format_logs') | to_json }}",
            header=['Parent Job ID',
                    'Supervisor Name',
                    'Status',
                    'Details',
                    'Job ID'],
            row=[
                "{{ dag_run_ecid() }}",
                "{{ item.properties.supervisorname }}",
                "{{ item.properties.status }}",
                "{{ item.properties.get('details','') }}",
                "{{ item.ecid }}",
            ]
        )

        log_logfilename_30 = rail.PythonOperator(
            task_id='log_logfilename_30',
            python_callable=lambda:  rail.render_template(
                "Log{{ dag_run_ecid() }}_supervisororg.csv")
        )

        log_checkifthereisfailedjobs_31 = rail.PythonOperator(
            task_id='log_checkifthereisfailedjobs_31',
            python_callable=lambda:  bool(rail.result('get_logged_errors'))
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_29')}}",
            output_file_name='{{ result("log_logfilename_30") }}',
            expires_in_seconds=7*24*60*60,
        )

        if_log_checkifthereisfailedjobs_31_present_36 = rail.IfOperator(
            task_id='if_log_checkifthereisfailedjobs_31_present_36',
            test='''{{ result('log_checkifthereisfailedjobs_31') | is_truthy }}''',
            yes_task="send_mail_error_37",
            no_task="send_mail_success_39",
        )

        send_mail_error_37 = rail.EmailOperator(
            task_id='send_mail_error_37',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} | Supervisor ORG sync Completed with Errors - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Supervisor ORG sync is completed with failures based on the file - '{{ result('log_logfilename_30') }}'. Please find the  link below to download the logs.
            <br /> <br /> <a href="{{ result('generate_download_link') }}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_success_39 = rail.EmailOperator(
            task_id='send_mail_success_39',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Supervisor ORG sync Completed Successfully - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Supervisor ORG sync is completed successfully based on the file - '{{ result('log_logfilename_30') }}'. Please find the download link below.
            <br /> <br /><a href="{{ result('generate_download_link') }}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_report_details >> generate_report_group >> if_generate_report_4_payload_not_has_valid_columns_5
        if_generate_report_4_payload_not_has_valid_columns_5 >> rail.Label(
            'Yes') >> stop_6
        if_generate_report_4_payload_not_has_valid_columns_5 >> rail.Label(
            'Yes') >> load_user_report_csv_data
        load_user_report_csv_data >> create_userlist_collection >> load_all_base_report_data >> create_log >> query_list_basereportdata_10 >> query_list_distinct_enabledsupervisorsfrom_replicon_11 >> get_all_department_groups_get_all_departments_13 >> trigger_dag_run_process_supervisor >> get_all_child_dag_runs
        get_all_child_dag_runs >> gather_child_logs >> format_logs >> get_logged_errors >> create_csv_lines_29 >> log_logfilename_30 >> log_checkifthereisfailedjobs_31 >> generate_download_link >> if_log_checkifthereisfailedjobs_31_present_36
        if_log_checkifthereisfailedjobs_31_present_36 >> rail.Label(
            'Yes') >> send_mail_error_37 >> log_to_sumo
        if_log_checkifthereisfailedjobs_31_present_36 >> rail.Label(
            'No') >> send_mail_success_39 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
