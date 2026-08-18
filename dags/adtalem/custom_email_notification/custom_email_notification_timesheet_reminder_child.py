
from datetime import timedelta
import rail
from adtalem.custom_email_notification.utils import request_payload, data_formatting, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_custom_email_notification_timesheet_reminder_child_{config.instance}',
        description=f'Live|Adtalem_Custom Email Notification_Child Timesheet Reminder {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
    ) as dag:
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )
        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: data_formatting.find_iten_by_displaytext_reminder(
                response, config.report1_name, config.report2_name)
        )
        has_all_report = rail.IfOperator(
            task_id='has_all_report',
            test="{{ result('get_all_report') | is_truthy}}",
            yes_task='get_timesheet_period_report_details',
            no_task='finish'
        )
        get_timesheet_period_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_period_report_details',
            report_name=config.report1_name
        )

        run_report_period_group_entry, run_report_period_group_exit = rail.run_report(
            group_id='timesheet_period_template_report_generation',
            report_params=request_payload.get_report1_params_for_reminder
        )
        load_csv_report1 = rail.LoadCSVFileOperator(
            task_id="load_csv_report1",
            document="{{ result('timesheet_period_template_report_generation.get_report_result').reportGenerationResults[0].payload }}",
        )
        csv_data = rail.PythonOperator(
            task_id="csv_data",
            python_callable=data_formatting.get_timesheet_period_detail_dict,
        )
        has_timesheet_period = rail.IfOperator(
            task_id='has_timesheet_period',
            test="{{ result('csv_data') | is_truthy}}",
            yes_task='get_not_submitted_timesheet_report_details',
            no_task='finish'
        )
        get_not_submitted_timesheet_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_not_submitted_timesheet_report_details',
            report_name=config.report2_name
        )
        run_report_for_not_submitted_timesheet_group_entry, run_report_for_not_submitted_timesheet_period_group_exit = rail.run_report(
            group_id='not_submitted_timesheet_report_generation',
            report_params=request_payload.get_report2_params_for_reminder
        )

        load_report_csv = rail.LoadCSVFileOperator(
            task_id="load_report_csv",
            document="{{ result('not_submitted_timesheet_report_generation.get_report_result').reportGenerationResults[0].payload }}",
        )
        compose_csv_with_headers = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_headers',
            source="{{ result('load_report_csv') }}",
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
        check_for_type = rail.IfOperator(
            task_id='check_for_type',
            test="{{ dag_run.conf['type'].lower() == 'regular' }}",
            yes_task="custom_email_notification_timesheet_reminder_for_regular_child",
            no_task="custom_email_notification_timesheet_reminder_for_accelerated_child",
        )

        custom_email_notification_timesheet_reminder_for_regular_child = rail.TriggerDagRunForEachItemOperator(
            task_id='custom_email_notification_timesheet_reminder_for_regular_child',
            retries=0,
            items=lambda: rail.result('get_query_data'),
            trigger_dag_id=f'{config.company_key}_custom_email_notification_individual_timesheet_reminder_for_regular_child_{config.instance}',
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

        wait_for_timesheet_reminder_for_regular_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_timesheet_reminder_for_regular_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("custom_email_notification_timesheet_reminder_for_regular_child") }}'
        )

        custom_email_notification_timesheet_reminder_for_accelerated_child = rail.TriggerDagRunForEachItemOperator(
            task_id='custom_email_notification_timesheet_reminder_for_accelerated_child',
            retries=0,
            items=lambda: rail.result('get_query_data'),
            trigger_dag_id=f'{config.company_key}_custom_email_notification_individual_timesheet_reminder_for_accelerated_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item, dag_run: {
                "supervisor": item.get('usersupervisorname'),
                "daterangevalue": item.get('timesheetperiod'),
                "supervisoremail": item.get('usersupervisoremailaddress'),
                "user": item.get('username'),
                "useremail": item.get('useremail'),
                "userfirstname": item.get('userfirstname'),
                "payrolldate": dag_run.conf['payrolldate'],
                "supervisoruri": item.get('supervisoruri'),
                "useruri": item.get('useruri'),
                "logid": rail.result('create_log')
            }
        )

        wait_for_timesheet_reminder_for_accelerated_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_timesheet_reminder_for_accelerated_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("custom_email_notification_timesheet_reminder_for_accelerated_child") }}'
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
            subject="{{ get_company_key() }} | {{ dag_run.conf.type }} Timesheet Reminder logs {{ result('get_status_subject_and_timestamp').get('subject') }} on {{ dag_run.conf.today }}",
            html_content="templates/emails/email_for_timesheet_reminder_format.html",
            params=None,
        )
        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            content="{{ result('write_log_file') }}",
            sftp_conn_id=config.client_sftp_conn_id,
            remote_filepath=config.log_path +
            "/PaycheckatRiskLogs_{{ dag_run_ecid() | replace(':', '-') }}.csv",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )
        create_log >> get_all_report
        get_all_report >> has_all_report >> rail.Label(
            'Yes') >> get_timesheet_period_report_details >> run_report_period_group_entry
        get_all_report >> has_all_report >> rail.Label(
            'No') >> finish
        run_report_period_group_exit >> load_csv_report1 >> csv_data >> has_timesheet_period
        has_timesheet_period >> rail.Label(
            'Yes') >> get_not_submitted_timesheet_report_details >> run_report_for_not_submitted_timesheet_group_entry
        has_timesheet_period >> rail.Label(
            'No') >> finish
        run_report_for_not_submitted_timesheet_period_group_exit >> load_report_csv >> compose_csv_with_headers >> create_collection_from_csv >> query_data_list >> get_query_data >> check_for_type
        check_for_type >> rail.Label(
            'Yes') >> custom_email_notification_timesheet_reminder_for_regular_child
        check_for_type >> rail.Label(
            'No') >> custom_email_notification_timesheet_reminder_for_accelerated_child
        custom_email_notification_timesheet_reminder_for_regular_child >> wait_for_timesheet_reminder_for_regular_child >> get_all_logs
        custom_email_notification_timesheet_reminder_for_accelerated_child >> wait_for_timesheet_reminder_for_accelerated_child >> get_all_logs
        get_all_logs >> check_logs_entries_greater_than_0
        check_logs_entries_greater_than_0 >> rail.Label(
            'Yes') >> write_log_file >> get_status_subject_and_timestamp >> create_collection_from_csv_logs >> query_list_numberof_records_processed_successfully >> query_list_numberof_records_not_processed >> query_list_numberof_records_skipped >> send_log_email >> upload_to_sftp >> finish
        check_logs_entries_greater_than_0 >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
