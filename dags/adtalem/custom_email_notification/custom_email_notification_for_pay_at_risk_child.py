
from datetime import timedelta
import rail
from adtalem.custom_email_notification.utils import request_payload, data_formatting, python_callable
null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_custom_email_notification_for_pay_at_risk_child_{config.instance}',
        description=f'Live|Adtalem_call recipe to send custom email notification for pay at risk {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
    ) as dag:
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: data_formatting.find_iten_by_displaytext_for_pay(
                response, config.report1_name, config.report2_name)
        )
        has_all_report = rail.IfOperator(
            task_id='has_all_report',
            test="{{ result('get_all_report').get('repor1_uri') | is_truthy}}",
            yes_task='check_for_type_regular',
            no_task='finish'
        )
        check_for_type_regular = rail.IfOperator(
            task_id='check_for_type_regular',
            test="{{ dag_run.conf['type'].lower() == 'regular' }}",
            yes_task="get_timesheet_period_report1_details",
            no_task="empty1",
        )
        get_timesheet_period_report1_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_period_report1_details',
            report_name=config.report1_name
        )
        run_report1_period_group_entry, run_report1_period_group_exit = rail.run_report(
            group_id='timesheet_period_template_report1_generation',
            report_params=request_payload.get_report1_params1_for_pay
        )
        load_csv_report1 = rail.LoadCSVFileOperator(
            task_id="load_csv_report1",
            document="{{ result('timesheet_period_template_report1_generation.get_report_result').reportGenerationResults[0].payload }}",
        )
        timesheet_period_report1_data = rail.PythonOperator(
            task_id="timesheet_period_report1_data",
            python_callable=data_formatting.get_timesheet_period_for_pay_detail_dict1,
        )

        check_for_type_accelerated = rail.IfOperator(
            task_id='check_for_type_accelerated',
            test="{{ dag_run.conf['type'].lower() == 'accelerated' }}",
            yes_task="get_timesheet_period_report2_details",
            no_task="has_timesheet_period",
        )

        get_timesheet_period_report2_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_period_report2_details',
            report_name=config.report1_name
        )
        run_report2_period_group_entry, run_report2_period_group_exit = rail.run_report(
            group_id='timesheet_period_template_report2_generation',
            report_params=request_payload.get_report1_params2_for_pay
        )
        load_csv_report2 = rail.LoadCSVFileOperator(
            task_id="load_csv_report2",
            document="{{ result('timesheet_period_template_report2_generation.get_report_result').reportGenerationResults[0].payload }}",
        )
        timesheet_period_report2_data = rail.PythonOperator(
            task_id="timesheet_period_report2_data",
            python_callable=data_formatting.get_timesheet_period_for_pay_detail_dict2,
        )
        has_timesheet_period = rail.IfOperator(
            task_id='has_timesheet_period',
            test="{{ result('timesheet_period_report1_data') | is_truthy or result('timesheet_period_report2_data') | is_truthy}}",
            yes_task='get_not_submitted_timesheet_report_details',
            no_task='finish'
        )
        get_not_submitted_timesheet_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_not_submitted_timesheet_report_details',
            report_name=config.report2_name
        )
        run_report_for_not_submitted_timesheet_group_entry, run_report_for_not_submitted_timesheet_group_exit = rail.run_report(
            group_id='not_submitted_timesheet_report_generation',
            report_params=request_payload.get_report2_params_for_pay
        )

        load_report_data_csv = rail.LoadCSVFileOperator(
            task_id="load_report_data_csv",
            document="{{ result('not_submitted_timesheet_report_generation.get_report_result').reportGenerationResults[0].payload }}",
        )
        compose_csv_with_headers = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_headers',
            source="{{ result('load_report_data_csv') }}",
            header=[
                'timesheetperiod',
                'username',
                'approvalstatus',
                'usersupervisorname',
                'usersupervisoremailaddress',
                'waitingonapprover',
                'useremail',
                'userfirstname',
                'useruri',
                'supervisoruri'
            ],
            row=[
                "{{ item['Timesheet Period'] }}",
                "{{ item['User Name']}}",
                "{{ item['Approval Status'] }}",
                "{{ item['User Supervisor Name (Current)'] }}",
                "{{ item['User Supervisor Email address'] }}",
                "{{ item['Waiting on Approver'] }}",
                "{{ item['User Email'] }}",
                "{{ item['User First Name'] }}",
                "{{ item['useruri'] }}",
                "{{ item['supervisoruri'] }}"

            ]
        )
        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('compose_csv_with_headers') }}",
            name="raw_data",
            # todo update this map from actual csv header for key name
            columns=[
                'timesheetperiod',
                'username',
                'approvalstatus',
                'usersupervisorname',
                'usersupervisoremailaddress',
                'waitingonapprover',
                'useremail',
                'userfirstname',
                'useruri',
                'supervisoruri'
            ]
        )

        query_data_list = rail.QueryCollectionOperator(
            task_id='query_data_list',
            query="""SELECT * FROM  raw_data""",
        )
        get_query_data = rail.PythonOperator(
            task_id='get_query_data',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_data_list")),
        )
        custom_email_notification_timesheet_reminder_for_pay_at_risk_child = rail.TriggerDagRunForEachItemOperator(
            task_id='custom_email_notification_timesheet_reminder_for_pay_at_risk_child',
            retries=0,
            items=lambda: rail.result('get_query_data'),
            trigger_dag_id=f'{config.company_key}_custom_email_notification_send_individual_for_pay_at_risk_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "supervisor": item.get('usersupervisorname'),
                "daterangevalue": item.get('timesheetperiod'),
                "supervisoremail": item.get('usersupervisoremailaddress'),
                "user": item.get('username'),
                "useremail": item.get('useremail'),
                "userfirstname": item.get('userfirstname'),
                "supervisoruri": item.get('supervisoruri'),
                "useruri": item.get('useruri'),
                "logid": rail.result('create_log')
            }
        )

        wait_for_timesheet_reminder_for_pay_at_risk_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_timesheet_reminder_for_pay_at_risk_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("custom_email_notification_timesheet_reminder_for_pay_at_risk_child") }}'
        )
        get_all_logs = rail.PythonOperator(
            task_id='get_all_logs',
            python_callable=python_callable.get_errror_logs
        )

        check_logs_entries_greater_than_0 = rail.IfOperator(
            task_id='check_logs_entries_greater_than_0',
            test='''{{ result('get_all_logs') | length > 0 }}''',
            yes_task="write_log_file",
            no_task="finish",
        )
        write_log_file = rail.WriteCSVFileOperator(
            task_id="write_log_file",
            source=lambda: rail.result('get_all_logs'),
            header=["jobid",
                    "date",
                    "type",
                    "username",
                    "supervisor",
                    "status"
                    ],
            row=lambda item: [
                item['JobID'],
                item['Date'],
                item['Type'],
                item['User Name | Supervisor Name'].split(
                    '|')[0] if item['User Name | Supervisor Name'] else "",
                item['User Name | Supervisor Name'].split(
                    '|')[-1] if item['User Name | Supervisor Name'] else "",
                item['Status']
            ]
        )
        get_status_subject_and_timestamp = rail.PythonOperator(
            task_id='get_status_subject_and_timestamp',
            python_callable=python_callable.get_details_for_email
        )

        create_collection_from_csv_logs = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv_logs',
            source="{{ result('write_log_file') }}",
            name="statuslist",
            # todo update this map from actual csv header for key name
            columns={
                'jobid': 'jobid',
                'date': 'date',
                'type': 'type',
                'username': 'username',
                'supervisor': 'supervisor',
                'status': 'status'
            }
        )

        query_list_numberof_records_processed_successfully = rail.QueryCollectionOperator(
            task_id='query_list_numberof_records_processed_successfully',
            query="SELECT * FROM  statuslist WHERE  statuslist.status LIKE '%Email Sent S%'",
        )

        query_list_numberof_records_not_processed = rail.QueryCollectionOperator(
            task_id='query_list_numberof_records_not_processed',
            query="SELECT * FROM  statuslist WHERE  statuslist.status LIKE '%Email Not Sent S%'",
        )

        query_list_numberof_records_skipped = rail.QueryCollectionOperator(
            task_id='query_list_numberof_records_skipped',
            query="SELECT * FROM  statuslist WHERE  statuslist.status LIKE 'Skipped%'",
        )

        send_log_email = rail.EmailOperator(
            task_id='send_log_email',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject="{{ get_company_key() }} | Paycheck at Risk Logs - {{ result('get_status_subject_and_timestamp').get('subject') }} on {{ dag_run.conf.today }}",
            html_content="templates/emails/email_for_timesheet_reminder_for_pay.html",
            params=None,
        )
        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            content="{{ result('write_log_file') }}",
            sftp_conn_id=config.client_sftp_conn_id,
            remote_filepath=config.log_path +
            "/PaycheckatRiskLogs_{{ dag_run_ecid() | replace(':', '-') }}.csv",
        )
        empty1 = rail.EmptyOperator(
            task_id='empty1',
        )
        finish = rail.EmptyOperator(
            task_id='finish',
        )

        create_log >> get_all_report >> has_all_report
        has_all_report >> rail.Label(
            'Yes') >> check_for_type_regular
        has_all_report >> rail.Label(
            'No') >> finish
        check_for_type_regular >> rail.Label(
            'Yes') >> get_timesheet_period_report1_details >> run_report1_period_group_entry >> run_report1_period_group_exit >> load_csv_report1 >> timesheet_period_report1_data >> empty1
        empty1 >> check_for_type_accelerated
        check_for_type_regular >> rail.Label(
            'No') >> empty1 >> check_for_type_accelerated
        check_for_type_accelerated >> rail.Label(
            'Yes') >> get_timesheet_period_report2_details >> run_report2_period_group_entry >> run_report2_period_group_exit >> load_csv_report2 >> timesheet_period_report2_data >> has_timesheet_period
        check_for_type_accelerated >> rail.Label(
            'No') >> has_timesheet_period
        has_timesheet_period >> rail.Label(
            'Yes') >> get_not_submitted_timesheet_report_details
        has_timesheet_period >> rail.Label(
            'No') >> finish
        get_not_submitted_timesheet_report_details >> run_report_for_not_submitted_timesheet_group_entry >> run_report_for_not_submitted_timesheet_group_exit >> load_report_data_csv >> compose_csv_with_headers >> create_collection_from_csv >> query_data_list >> get_query_data

        get_query_data >> custom_email_notification_timesheet_reminder_for_pay_at_risk_child >> wait_for_timesheet_reminder_for_pay_at_risk_child >> get_all_logs
        get_all_logs >> check_logs_entries_greater_than_0
        check_logs_entries_greater_than_0 >> rail.Label(
            'Yes') >> write_log_file >> get_status_subject_and_timestamp >> create_collection_from_csv_logs >> query_list_numberof_records_processed_successfully >> query_list_numberof_records_not_processed >> query_list_numberof_records_skipped >> send_log_email >> upload_to_sftp >> finish
        check_logs_entries_greater_than_0 >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
