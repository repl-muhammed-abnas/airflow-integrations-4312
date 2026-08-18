# pylint: disable=too-many-statements
from datetime import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from tungstenconstructionllc.payroll_export_replicon_to_sftp.utils.python_callable import get_today
from tungstenconstructionllc.payroll_export_replicon_to_sftp.utils import python_callable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'tungstenconstructionllcafmig_payroll_export_master_{config.instance}',
        description=f'tungstenconstructionllcafmig_payroll_export_master_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        is_daterange_absent = rail.IfOperator(
            task_id="is_daterange_absent",
            test="{{ dag_run.conf.webhook.data.get('dateRange') == None }}",
            yes_task="send_no_daterange_selected_mail",
            no_task="is_daterange_null"
        )

        is_daterange_null = rail.IfOperator(
            task_id="is_daterange_null",
            test="{{ dag_run.conf.webhook.data.dateRange | is_falsy }}",
            yes_task="send_daterange_contains_null_mail",
            no_task="is_approval_status_absent"
        )

        is_approval_status_absent = rail.IfOperator(
            task_id="is_approval_status_absent",
            test="{{ dag_run.conf.webhook.data.timesheetApprovalStatusIds | is_falsy  }}",
            yes_task="send_no_approval_status_mail",
            no_task="get_date_range_data"
        )

        get_date_range_data = rail.PythonOperator(
            task_id="get_date_range_data",
            python_callable=python_callable.get_daterange_data
        )

        is_date_range_less_than_0 = rail.IfOperator(
            task_id="is_date_range_less_than_0",
            test="{{ result('get_date_range_data').daterange_diff < 0}}",
            yes_task="send_end_date_before_start_date_mail",
            no_task="is_date_range_more_than_60"
        )

        is_date_range_more_than_60 = rail.IfOperator(
            task_id="is_date_range_more_than_60",
            test="{{ result('get_date_range_data').daterange_diff > 60}}",
            yes_task="send_daterange_more_than_60_days_mail",
            no_task="get_report_uri"
        )

        get_report_uri = rail.RepliconServiceOperator(
            task_id="get_report_uri",
            endpoint="/services/reportservice1.svc/GetAllReports",
            data_handler=lambda response:
            {
                "payroll_report_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Customization Payroll Report', 'uri', ''),
                "expense_report_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Customization Expense Report', 'uri', ''),
                "timesheet_report_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Customization Timesheet Comments', 'uri', '')
            }
        )

        get_customization_payroll_report_data_uri = rail.RepliconServiceOperator(
            task_id="get_customization_payroll_report_data_uri",
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result('get_report_uri')['payroll_report_uri']
            },
            data_handler=lambda response:{
                "userfilter_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', ''),
                "entrydatefilter_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri', ''),

            }
        )

        get_customization_expense_report_data_uri = rail.RepliconServiceOperator(
            task_id="get_customization_expense_report_data_uri",
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result('get_report_uri')['expense_report_uri']
            },
            data_handler=lambda response:{
                "userfilter_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', ''),
                "daterangefilter_incurreduri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter_IncurredDate', 'uri', ''),
                "approvalstatus_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', '')
            }
        )

        get_customization_timesheet_comments_report_data_uri = rail.RepliconServiceOperator(
            task_id="get_customization_timesheet_comments_report_data_uri",
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result('get_report_uri')['timesheet_report_uri']
            },
            data_handler=lambda response:{
                "userfilter_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', ''),
                "entrydatefilter_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri', ''),
                "approvalstatus_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', '')
            }
        )

        is_userids_present = rail.IfOperator(
            task_id="is_userids_present",
            test="{{ dag_run.conf.webhook.data.userIds | is_truthy }}",
            yes_task="add_userids_to_lists",
            no_task="add_entry_dates_to_lists"
        )

        add_userids_to_lists = rail.PythonOperator(
            task_id="add_userids_to_lists",
            python_callable=python_callable.add_userids
        )

        add_entry_dates_to_lists = rail.PythonOperator(
            task_id="add_entry_dates_to_lists",
            python_callable=python_callable.add_entry_dates
        )

        add_approval_status_to_lists = rail.PythonOperator(
            task_id="add_approval_status_to_lists",
            python_callable=python_callable.add_approval_status
        )

        run_payroll_report_entry, run_payroll_report_exit = rail.run_report(
            group_id='run_report_payroll',
            report_params=python_callable.get_payroll_report_params
        )

        is_payroll_report_failed = rail.IfOperator(
            task_id="is_payroll_report_failed",
            test='{{result("run_report_payroll.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_payroll_report_generation",
            no_task="is_payroll_report_has_no_data"
        )

        fail_payroll_report_generation = rail.FailOperator(
            task_id="fail_payroll_report_generation",
            message="{{result('run_report_payroll.get_report_result').reportGenerationResults[0].error}}"
        )

        is_payroll_report_has_no_data = rail.IfOperator(
            task_id="is_payroll_report_has_no_data",
            test="{{ result('run_report_payroll.get_report_result').reportGenerationResults[0].payload | starts_with('No Data')}}",
            yes_task='send_no_data_payroll_base_report_email',
            no_task='load_payroll_report_data'
        )

        load_payroll_report_data = rail.LoadCSVFileOperator(
            task_id='load_payroll_report_data',
            document="{{ result('run_report_payroll.get_report_result').reportGenerationResults[0].payload }}",
        )

        payroll_report_data_collection = rail.CreateCollectionOperator(
            task_id="payroll_report_data_collection",
            source="{{ result('load_payroll_report_data') }}",
            columns={
                "Login Name": "loginname",
                "User First Name": "firstname",
                "User Last Name": "lastname",
                "Current Hourly Payroll": "payrate",
                "Pay Code Code": "paycode",
                "Pay Code Hours": "paycodehours",
                "Regular Hours": "regularhours",
                "OverTime Hours": "overtimehours",
                "TimeOff Hours": "timeoffhours",
                "Approval Status": "approvalstatus",
                "Employee Type": "employeetype",
                "Equipment Cost": "equipmentcost"
            },
            name="payrollinput"
        )

        get_approval_status_for_query = rail.PythonOperator(
            task_id="get_approval_status_for_query",
            python_callable=python_callable.query_statement_for_payroll_validated_status_data
        )

        query_from_payroll_input = rail.QueryCollectionOperator(
            task_id="query_from_payroll_input",
            query="""SELECT * FROM payrollinput WHERE approvalstatus IN {{ result('get_approval_status_for_query') }} """,
            name="payroll_input_data"
        )

        query_distinct_loginname_payroll_data = rail.QueryCollectionOperator(
            task_id="query_distinct_loginname_payroll_data",
            query="""SELECT DISTINCT loginname FROM payroll_input_data""",
            name="payroll_distinct_loginname_data"
        )

        query_distinct_payroll_data = rail.QueryCollectionOperator(
            task_id="query_distinct_payroll_data",
            query="""SELECT DISTINCT loginname, paycodehours, regularhours, overtimehours, timeoffhours FROM payroll_input_data""",
            name="payroll_validate_status_data"
        )

        run_expense_report_entry, run_expense_report_exit = rail.run_report(
            group_id='run_report_expense',
            report_params=python_callable.get_expense_report_params
        )

        is_expense_report_failed = rail.IfOperator(
            task_id="is_expense_report_failed",
            test='{{result("run_report_expense.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_expense_report_generation",
            no_task="is_expense_report_has_no_data"
        )

        fail_expense_report_generation = rail.FailOperator(
            task_id="fail_expense_report_generation",
            message="{{result('run_report_expense.get_report_result').reportGenerationResults[0].error}}"
        )

        is_expense_report_has_no_data = rail.IfOperator(
            task_id="is_expense_report_has_no_data",
            test="{{ result('run_report_expense.get_report_result').reportGenerationResults[0].payload | starts_with('No Data')}}",
            yes_task='send_no_data_expense_base_report_email',
            no_task='load_expense_report_data'
        )

        load_expense_report_data = rail.LoadCSVFileOperator(
            task_id='load_expense_report_data',
            document="{{ result('run_report_expense.get_report_result').reportGenerationResults[0].payload }}",
        )

        expense_report_data_collection = rail.CreateCollectionOperator(
            task_id="expense_report_data_collection",
            source="{{ result('load_expense_report_data') }}",
            columns={
                "Login Name": "loginname",
                "Expense Code": "expensecode",
                "Number of Units": "units",
                "Amount": "amount"
            },
            name="expenseinput"
        )

        query_from_expense_input = rail.QueryCollectionOperator(
            task_id="query_from_expense_input",
            query="""SELECT * FROM expenseinput""",
            name="expense_input_data"
        )

        run_timesheet_report_entry, run_timesheet_report_exit = rail.run_report(
            group_id='run_report_timesheet',
            report_params=python_callable.get_timesheet_report_params
        )

        load_timesheet_report_data = rail.LoadCSVFileOperator(
            task_id='load_timesheet_report_data',
            document="{{ result('run_report_timesheet.get_report_result').reportGenerationResults[0].payload }}",
        )

        timesheet_report_data_collection = rail.CreateCollectionOperator(
            task_id="timesheet_report_data_collection",
            source="{{ result('load_timesheet_report_data') }}",
            columns={
                "Login Name": "loginname",
                "Approval Status": "approvalstatus",
                "Approver's Comments": "comments"
            },
            name="timesheetcomments"
        )

        query_from_timesheet_comments = rail.QueryCollectionOperator(
            task_id="query_from_timesheet_comments",
            query="""SELECT * FROM timesheetcomments""",
            name="timesheet_input"
        )

        is_timesheet_comment_data_present = rail.IfOperator(
            task_id='is_timesheet_comment_data_present',
            test='{{ result("query_from_timesheet_comments", "length") > 0 }}',
            yes_task='compose_csv_with_header',
            no_task='send_no_data_available_to_export_mail'
        )

        compose_csv_with_header = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_header',
            source="{{ result('query_distinct_loginname_payroll_data') }}",
            header=[
                'Employee Name',
                'Employee Rate/Salary',
                'Equipment Cost',
                'Vacation Hours',
                'Regular Hours Worked',
                'Overtime Hours Worked',
                'Total Hours Worked',
                'Additions to Pay (Perdiem)',
                'Additions to Pay (Truck Allowance)',
                'Additions to Pay (Medical)',
                'Additions to Pay (Supplies)',
                'Additions to Pay (Loan)',
                'Days for Per Diem (Units)',
                'Notes'
            ],
            row=python_callable.get_row
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda dag_run: get_dagrun_ecid(dag_run).replace(':','_') + '_' +
            datetime.today().strftime('%m%d%YT%H%M') + "_payrollperdiemextract.csv"
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content="{{ result('compose_csv_with_header') }}",
            remote_filepath= config.csv_filepath + '/' + "{{ result('get_file_name') }}",
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Error',
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('filter_master_log', 'length') > 0 }}",
            yes_task='send_error_mail',
            no_task='generate_download_link'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_csv_with_header')}}",
            output_file_name="{{ result('get_file_name') }}",
            expires_in_seconds=7*24*60*60,
        )

        send_mail_with_cshare = rail.EmailOperator(
            task_id='send_mail_with_cshare',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject='''{{ get_company_key() }}  | Custom Payroll Extract processed successfully ''',
            html_content="templates/email/cshare_email.html",
            params=None,
        )

        send_no_daterange_selected_mail = rail.EmailOperator(
            task_id='send_no_daterange_selected_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on ' + get_today(),
            html_content="templates/email/no_daterange_selected.html"
        )

        send_daterange_contains_null_mail = rail.EmailOperator(
            task_id='send_daterange_contains_null_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on  ' + get_today(),
            html_content="templates/email/daterange_conatins_null.html"
        )

        send_no_approval_status_mail = rail.EmailOperator(
            task_id='send_no_approval_status_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on  ' + get_today(),
            html_content="templates/email/no_approval_status_mail.html"
        )

        send_end_date_before_start_date_mail = rail.EmailOperator(
            task_id='send_end_date_before_start_date_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on  ' + get_today(),
            html_content="templates/email/end_date_before_start_date_email.html"
        )

        send_daterange_more_than_60_days_mail = rail.EmailOperator(
            task_id='send_daterange_more_than_60_days_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on  ' + get_today(),
            html_content="templates/email/daterange_more_than_60_days.html"
        )

        send_no_data_payroll_base_report_email = rail.EmailOperator(
            task_id='send_no_data_payroll_base_report_email',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on  ' + get_today(),
            html_content="templates/email/no_data_payroll_base_report_email.html"
        )

        send_no_data_expense_base_report_email = rail.EmailOperator(
            task_id='send_no_data_expense_base_report_email',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on  ' + get_today(),
            html_content="templates/email/no_data_expense_base_report_email.html"
        )

        send_no_data_available_to_export_mail = rail.EmailOperator(
            task_id='send_no_data_available_to_export_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Export skipped on  ' + get_today(),
            html_content="templates/email/no_data_available_to_export_email.html"
        )

        send_error_mail = rail.EmailOperator(
            task_id='send_error_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom Payroll Extract File Upload Failure ' + get_today(),
            html_content="templates/email/error_email.html",
            files=[
                    ("{{ result('get_file_name') }}", '{{result("compose_csv_with_header")}}')
            ]
        )

        is_daterange_absent >> rail.Label(
            "Yes") >> send_no_daterange_selected_mail
        is_daterange_absent >> rail.Label("No") >> is_daterange_null >> rail.Label(
            "Yes") >> send_daterange_contains_null_mail
        is_daterange_null >> rail.Label("No") >> is_approval_status_absent >> rail.Label(
            "No") >> get_date_range_data >> is_date_range_less_than_0

        is_approval_status_absent >> rail.Label(
            "Yes") >> send_no_approval_status_mail

        is_date_range_less_than_0 >> rail.Label(
            "Yes") >> send_end_date_before_start_date_mail
        is_date_range_less_than_0 >> rail.Label("No") >> is_date_range_more_than_60 >> rail.Label("Yes") >> \
            send_daterange_more_than_60_days_mail

        is_date_range_more_than_60 >> rail.Label("No") >> get_report_uri >> \
            get_customization_payroll_report_data_uri >> get_customization_expense_report_data_uri >> \
            get_customization_timesheet_comments_report_data_uri >> is_userids_present

        is_userids_present >> rail.Label(
            "Yes") >> add_userids_to_lists >> add_entry_dates_to_lists

        is_userids_present >> rail.Label("No") >> add_entry_dates_to_lists

        add_entry_dates_to_lists >> add_approval_status_to_lists >> run_payroll_report_entry

        run_payroll_report_exit >> is_payroll_report_failed >> rail.Label(
            "Yes") >> fail_payroll_report_generation
        run_payroll_report_exit >> is_payroll_report_failed >> rail.Label(
            "No") >> is_payroll_report_has_no_data

        is_payroll_report_has_no_data >> rail.Label(
            "Yes") >> send_no_data_payroll_base_report_email
        is_payroll_report_has_no_data >> rail.Label("No") >> load_payroll_report_data >> payroll_report_data_collection >> \
            get_approval_status_for_query >> query_from_payroll_input >> query_distinct_loginname_payroll_data >> \
                query_distinct_payroll_data >> run_expense_report_entry

        run_expense_report_exit >> is_expense_report_failed >> rail.Label(
            "Yes") >> fail_expense_report_generation
        run_expense_report_exit >> is_expense_report_failed >> rail.Label(
            "No") >> is_expense_report_has_no_data

        is_expense_report_has_no_data >> rail.Label(
            "Yes") >> send_no_data_expense_base_report_email
        is_expense_report_has_no_data >> rail.Label("No") >> load_expense_report_data >> expense_report_data_collection >> \
            query_from_expense_input >> run_timesheet_report_entry

        run_timesheet_report_exit >> load_timesheet_report_data >> timesheet_report_data_collection >> \
            query_from_timesheet_comments >> is_timesheet_comment_data_present

        is_timesheet_comment_data_present >> rail.Label(
            "No") >> send_no_data_available_to_export_mail

        is_timesheet_comment_data_present >> rail.Label("Yes") >> compose_csv_with_header >> get_file_name >> \
            upload_csv_to_sftp >> filter_master_log >> any_records_failed >> rail.Label("Yes") >> \
                send_error_mail >> log_to_sumo >> can_fail_dag >> fail_dagrun

        any_records_failed >> rail.Label("No") >> generate_download_link >> send_mail_with_cshare

    return dag


rail.for_each_instance(create_dag)
