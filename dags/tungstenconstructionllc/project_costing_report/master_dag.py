# pylint: disable=too-many-statements line-too-long unnecessary-lambda
from datetime import timedelta
import json
import rail
from tungstenconstructionllc.project_costing_report.utils import python_callable, response_filter, request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'tungstenconstructionllc_project_costing_report_master_{config.instance}',
        description=f'TungstenConstructionLLC_Project_Costing_Report_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_project_available = rail.IfOperator(
            task_id='is_project_available',
            test=lambda dag_run: bool(dag_run.conf['webhook']['data']['projectIds']),
            yes_task="is_daterange_available",
            no_task="send_no_project_selected_mail"
        )

        send_no_project_selected_mail = rail.EmailOperator(
            task_id='send_no_project_selected_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report skipped {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_no_project_selected_mail.html"
        )

        is_daterange_available = rail.IfOperator(
            task_id='is_daterange_available',
            test=lambda dag_run: bool(dag_run.conf['webhook']['data']['dateRange']),
            yes_task="is_daterange_does_not_contains_null",
            no_task="send_no_date_range_selected_mail"
        )

        send_no_date_range_selected_mail = rail.EmailOperator(
            task_id='send_no_date_range_selected_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report skipped {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_no_date_range_selected_mail.html"
        )

        is_daterange_does_not_contains_null = rail.IfOperator(
            task_id='is_daterange_does_not_contains_null',
            test=lambda dag_run: python_callable.check_if_daterange_does_not_contains_null(dag_run),
            yes_task="get_start_end_date",
            no_task="send_incomplete_date_range_mail"
        )

        send_incomplete_date_range_mail = rail.EmailOperator(
            task_id='send_incomplete_date_range_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report skipped {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_incomplete_date_range_mail.html"
        )

        get_start_end_date = rail.PythonOperator(
            task_id="get_start_end_date",
            python_callable=lambda dag_run: response_filter.get_start_end_date(dag_run)
        )

        check_day_difference = rail.IfOperator(
            task_id='check_day_difference',
            test=lambda dag_run: python_callable.get_start_end_date_difference(dag_run.conf['webhook']['data']['dateRange']) < 0,
            yes_task="send_end_date_is_less_than_start_date_mail",
            no_task="get_file_format_script_uri"
        )

        send_end_date_is_less_than_start_date_mail = rail.EmailOperator(
            task_id='send_end_date_is_less_than_start_date_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report skipped {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_end_date_is_less_than_start_date_mail.html"
        )

        get_file_format_script_uri = rail.RepliconServiceOperator(
            task_id='get_file_format_script_uri',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: response_filter.get_file_format_uri(response,)
        )

        create_payroll_download_batch = rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch',
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=lambda dag_run: python_callable.get_request_body_payroll_download_batch(dag_run)
        )

        execute_payroll_download_batch, wait_for_payroll_download_batch = rail.batch_execution(
            'execute_payroll_download_batch', create_payroll_download_batch.task_id
        )

        get_payroll_download_batch_results = rail.RepliconServiceOperator(
            task_id='get_payroll_download_batch_results',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch') }}"
            }
        )

        download_payroll_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_payroll_file_from_url",
            url="{{ result('get_payroll_download_batch_results').downloadUrl }}"
        )

        load_payroll_file = rail.LoadCSVFileOperator(
            task_id="load_payroll_file",
            document="{{ result('download_payroll_file_from_url') }}"
        )

        if_csv_has_no_data = rail.IfOperator(
            task_id='if_csv_has_no_data',
            test="{{result('load_payroll_file') | load_all_records() | length == 0 }}",
            yes_task="send_no_data_to_export_mail",
            no_task="create_csv_format_payrolldata",
        )

        send_no_data_to_export_mail = rail.EmailOperator(
            task_id='send_no_data_to_export_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report - No data to export {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_no_data_to_export_mail.html"
        )

        create_csv_format_payrolldata=rail.WriteCSVFileOperator(
            task_id='create_csv_format_payrolldata',
            source="{{ result('load_payroll_file') }}",
            header=[
                'loginname',
                'timsheetstart',
                'timesheetend',
                'projectname',
                'rthours',
                'othours'
            ],
            row=request_payload.get_row_data
        )

        hourly_cost_report_for_project_costing = rail.RepliconReportDetailsOperator(
            task_id='hourly_cost_report_for_project_costing',
            report_name=config.hourly_cost_report_for_project_costing
        )

        if_uri_present_in_report_details = rail.IfOperator(
            task_id='if_uri_present_in_report_details',
            test="{{ result('hourly_cost_report_for_project_costing').uri | is_truthy }}",
            yes_task='get_entry_date_filter_and_project_filter_uri',
            no_task='failed_dag_with_error'
        )

        get_entry_date_filter_and_project_filter_uri = rail.PythonOperator(
            task_id='get_entry_date_filter_and_project_filter_uri',
            python_callable=python_callable.get_entry_date_filter_and_project_filter_uri
        )

        failed_dag_with_error = rail.FailOperator(
            task_id='failed_dag_with_error',
            message='''Report not found - Hourly Cost report for Project Costing'''
        )

        create_report_filter_1_list = rail.PythonOperator(
            task_id='create_report_filter_1_list',
            python_callable=lambda dag_run: python_callable.create_report_filter_1_list(dag_run)
        )

        hourly_cost_report_entry, hourly_cost_report_exit = rail.run_report(
            group_id='hourly_cost_report_dag_run',
            report_params=lambda dag_run: {
                "reportParameters": [
                   {
                       "reportUri": rail.result('hourly_cost_report_for_project_costing').get('uri'),
                       "filterValues": json.loads(rail.result('create_report_filter_1_list')),
                       "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                   }
                ]
            }
        )

        if_error_in_report_batch_result = rail.IfOperator(
            task_id='if_error_in_report_batch_result',
            test="{{ result('hourly_cost_report_dag_run.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='failed_dag_with_report_batch_result_error',
            no_task='has_empty_report_data'
        )

        failed_dag_with_report_batch_result_error = rail.FailOperator(
            task_id='failed_dag_with_report_batch_result_error',
            message="Error generating report - {{ result('hourly_cost_report_dag_run.get_report_result').reportGenerationResults[0].error }}"
        )

        has_empty_report_data = rail.IfOperator(
            task_id='has_empty_report_data',
            test=lambda: rail.result("hourly_cost_report_dag_run.get_report_result")[
                'reportGenerationResults'][0]['payload'].startswith("No Data"),
            yes_task="send_nodata_mail",
            no_task="load_report_csv_file",
        )

        send_nodata_mail = rail.EmailOperator(
            task_id='send_nodata_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report - No data to export {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_no_data_to_export_mail.html"
        )

        load_report_csv_file = rail.LoadCSVFileOperator(
            task_id="load_report_csv_file",
            document="{{ result('hourly_cost_report_dag_run.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_csv_format_hourly_cost_data=rail.WriteCSVFileOperator(
            task_id='create_csv_format_hourly_cost_data',
            source="{{ result('load_report_csv_file') }}",
            header=[
                'client',
                'project',
                'timesheetstartdate',
                'timesheetenddate',
                'totalhours',
                'username',
                'loginname',
                'hourlycostamount',
                'equipmentcost'
            ],
            row=request_payload.get_row_hourly_cost_data
        )

        expense_report_for_project_costing = rail.RepliconReportDetailsOperator(
            task_id='expense_report_for_project_costing',
            report_name=config.expense_report_for_project_costing
        )

        if_uri_present_in_expense_report_details = rail.IfOperator(
            task_id='if_uri_present_in_expense_report_details',
            test="{{ result('expense_report_for_project_costing').uri | is_truthy }}",
            yes_task='get_date_range_project_expense_type_filter_uri',
            no_task='failed_dag_with_uri_not_present'
        )

        failed_dag_with_uri_not_present = rail.FailOperator(
            task_id='failed_dag_with_uri_not_present',
            message='''Report not found - Expense report for Project Costing'''
        )

        get_date_range_project_expense_type_filter_uri = rail.PythonOperator(
            task_id='get_date_range_project_expense_type_filter_uri',
            python_callable=python_callable.get_date_range_project_expense_type_filter_uri
        )

        get_all_expense_codes = rail.RepliconServiceOperator(
            task_id='get_all_expense_codes',
            endpoint="/services/expenseService1.svc/GetAllExpenseCodes",
            data=None
        )

        per_diem_expense_code_id=rail.PythonOperator(
            task_id='per_diem_expense_code_id',
            python_callable= python_callable.get_per_diem_expense_code_id
        )

        add_value_to_filter_1_list = rail.PythonOperator(
            task_id='add_value_to_filter_1_list',
            python_callable=python_callable.add_value_to_filter_1_list,
            op_args=["{{ result('get_date_range_project_expense_type_filter_uri').ExpenseTypeFilter }}", "{{ result('per_diem_expense_code_id') }}"]
        )

        add_values_report_filter_2_list = rail.PythonOperator(
            task_id='add_values_report_filter_2_list',
            python_callable=lambda dag_run: python_callable.add_values_report_filter_2_list(dag_run)
        )

        expense_report_entry, expense_report_exit = rail.run_report(
            group_id='expense_report_dag_run',
            report_params=lambda dag_run: {
                "reportParameters": [
                   {
                       "reportUri": rail.result('expense_report_for_project_costing').get('uri'),
                       "filterValues": json.loads(rail.result('add_values_report_filter_2_list')),
                       "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                   }
                ]
            }
        )

        if_error_in_expense_report_batch_result = rail.IfOperator(
            task_id='if_error_in_expense_report_batch_result',
            test="{{ result('expense_report_dag_run.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='failed_dag_with_expense_report_batch_result_error',
            no_task='load_expense_report_csv_file'
        )

        failed_dag_with_expense_report_batch_result_error = rail.FailOperator(
            task_id='failed_dag_with_expense_report_batch_result_error',
            message="Error generating report - {{ result('expense_report_dag_run.get_report_result').reportGenerationResults[0].error }}"
        )

        load_expense_report_csv_file = rail.LoadCSVFileOperator(
            task_id="load_expense_report_csv_file",
            document="{{ result('expense_report_dag_run.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_collection_from_payroll_data_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_payroll_data_csv',
            source="{{ result('create_csv_format_payrolldata') }}",
            name="payrolldata",
            columns=[
                'loginname',
                'timsheetstart',
                'timesheetend',
                'projectname',
                'rthours',
                'othours'
            ]
        )

        create_collection_from_hourly_cost_data_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_hourly_cost_data_csv',
            source="{{ result('create_csv_format_hourly_cost_data') }}",
            name="hourly_cost_data",
            columns=[
                'client',
                'project',
                'timesheetstartdate',
                'timesheetenddate',
                'totalhours',
                'username',
                'loginname',
                'hourlycostamount',
                'equipmentcost'
            ]
        )

        query_data_list = rail.QueryCollectionOperator(
            task_id='query_data_list',
            query="""SELECT * FROM payrolldata WHERE projectname IN (SELECT DISTINCT project FROM hourly_cost_data) AND projectname IS NOT NULL""",
        )
        get_query_data = rail.PythonOperator(
            task_id='get_query_data',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_data_list")),
        )

        if_query_list_data_records_rows_is_0 = rail.IfOperator(
            task_id='if_query_list_data_records_rows_is_0',
            test='''{{ result('get_query_data') | length == 0 }}''',
            yes_task="send_no_project_data_mail",
            no_task="if_query_list_data_records_rows_is_greater_than_0",
        )

        send_no_project_data_mail = rail.EmailOperator(
            task_id='send_no_project_data_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report - No data to export  {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_no_project_data_mail.html"
        )

        if_query_list_data_records_rows_is_greater_than_0 = rail.IfOperator(
            task_id='if_query_list_data_records_rows_is_greater_than_0',
            test='''{{ result('get_query_data') | length > 0 }}''',
            yes_task="create_csv_format_expense_data",
            no_task="project_costing_generate_report_lookup_table",
        )

        create_csv_format_expense_data=rail.WriteCSVFileOperator(
            task_id='create_csv_format_expense_data',
            source="{{ result('load_expense_report_csv_file') }}",
            header=[
                'clientname',
                'projectname',
                'username',
                'loginname',
                'expensecode',
                'trackingnumber',
                'incurreddate',
                'amount',
                'approvalstatus'
            ],
            row=request_payload.get_row_expense_data
        )

        create_collection_from_expense_data_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_expense_data_csv',
            source="{{ result('create_csv_format_expense_data') }}",
            name="expense_data",
            columns=[
                'clientname',
                'projectname',
                'username',
                'loginname',
                'expensecode',
                'trackingnumber',
                'incurreddate',
                'amount',
                'approvalstatus'
            ]
        )

        project_costing_generate_report_lookup_table = rail.CreateLogOperator(
            task_id='project_costing_generate_report_lookup_table'
        )

        foreach_query_data_list_child = rail.TriggerDagRunForEachItemOperator(
            task_id='foreach_query_data_list_child',
            retries=0,
            items=lambda: rail.result('get_query_data'),
            trigger_dag_id=f'tungstenconstructionllc_project_costing_hourly_cost_report_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "loginname": item.get('loginname'),
                "timsheetstart": item.get('timsheetstart'),
                "timesheetend": item.get('timesheetend'),
                "projectname": item.get('projectname'),
                "rthours": item.get('rthours'),
                "othours": item.get('othours'),
                "lookup_table": rail.result('project_costing_generate_report_lookup_table'),
                "parse_csv": rail.result('load_expense_report_csv_file'),
                "jobid": "{{ dag_run_ecid().rsplit(':', 1)[0] }}",
            }
        )

        wait_for_foreach_query_data_list_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_foreach_query_data_list_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("foreach_query_data_list_child") }}'
        )

        if_entry_present = rail.IfOperator(
            task_id='if_entry_present',
            test="{{ result('project_costing_generate_report_lookup_table') | load_all_records() | length > 0 }}",
            yes_task='query_distinct_projects_and_timesheets',
            no_task='send_no_project_data_in_lookup_table_mail'
        )

        send_no_project_data_in_lookup_table_mail = rail.EmailOperator(
            task_id='send_no_project_data_in_lookup_table_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report - No data to export  {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_no_project_data_mail.html"
        )

        query_distinct_projects_and_timesheets = rail.QueryCollectionOperator(
            task_id='query_distinct_projects_and_timesheets',
            query="""SELECT DISTINCT projectname, timsheetstart, timesheetend FROM payrolldata WHERE projectname IS NOT NULL ORDER BY projectname ASC, timsheetstart ASC""",
        )
        get_distinct_projects_and_timesheets_data = rail.PythonOperator(
            task_id='get_distinct_projects_and_timesheets_data',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_distinct_projects_and_timesheets")),
        )

        accumulate_items_to_data_per_project_and_timesheet_list = rail.PythonOperator(
            task_id='accumulate_items_to_data_per_project_and_timesheet_list',
            python_callable=python_callable.accumulate_items_to_data_per_project_and_timesheet_list,
            op_kwargs={"jobid": "{{ dag_run_ecid().rsplit(':', 1)[0] }}"}
        )

        foreach_accumulate_items_list = rail.ForEachOperator(
            task_id='foreach_accumulate_items_list',
            items=lambda: rail.result('accumulate_items_to_data_per_project_and_timesheet_list'),
            start_task='if_project_present_in_accumulate_items',
            end_task='foreach_accumulate_items_list_end'
        )

        if_project_present_in_accumulate_items = rail.IfOperator(
            task_id='if_project_present_in_accumulate_items',
            test="{{ result('foreach_accumulate_items_list').project | is_truthy }}",
            yes_task='add_success_entries_lookup_table',
            no_task='foreach_accumulate_items_list_end'
        )

        add_success_entries_lookup_table = rail.WriteLogOperator(
            task_id='add_success_entries_lookup_table',
            log="{{ result('project_costing_generate_report_lookup_table') }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "jobid": rail.result('foreach_accumulate_items_list').get('jobid'),
                "client": rail.result('foreach_accumulate_items_list').get('client'),
                "project": rail.result('foreach_accumulate_items_list').get('project'),
                "timesheetperiod": python_callable.get_timesheetperiod(rail.result('foreach_accumulate_items_list')),
                "project_RT_hours": None,
                "project_OT_hours": None,
                "hourlycost": None,
                "equipmentcost": config.CURRENCY_PREFIX + str(rail.result('foreach_accumulate_items_list').get('weeklytotals')),
                "gross income": config.CURRENCY_PREFIX + str(rail.result('foreach_accumulate_items_list').get('grossincometotals')),
                "perdiem_amount": config.CURRENCY_PREFIX + str(rail.result('foreach_accumulate_items_list').get('perdiemtotals'))
            }
        )

        foreach_accumulate_items_list_end = rail.EmptyOperator(
            task_id='foreach_accumulate_items_list_end',
        )

        query_distinct_projects = rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            query="""SELECT DISTINCT project FROM hourly_cost_data WHERE project IS NOT NULL ORDER BY project ASC""",
        )
        get_distinct_projects_data = rail.PythonOperator(
            task_id='get_distinct_projects_data',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_distinct_projects")),
        )

        create_final_data_lookup_table = rail.CreateLogOperator(
            task_id='create_final_data_lookup_table'
        )

        foreach_distinct_projects_data_list = rail.ForEachOperator(
            task_id='foreach_distinct_projects_data_list',
            items=lambda: rail.result('get_distinct_projects_data'),
            start_task='distinct_projects_in_lookup_table',
            end_task='foreach_distinct_projects_data_list_end'
        )

        distinct_projects_in_lookup_table = rail.FilterLogEntriesOperator(
            task_id='distinct_projects_in_lookup_table',
            log="{{result('project_costing_generate_report_lookup_table')}}",
            properties={
                'jobid': "final_{{ dag_run_ecid().rsplit(':', 1)[0] }}",
                'project': "{{ result('foreach_distinct_projects_data_list').project }}",
            }
        )

        load_all_distinct_projects_in_lookup_table = rail.PythonOperator(
            task_id='load_all_distinct_projects_in_lookup_table',
            python_callable=lambda: rail.load_all_records(
                rail.result("distinct_projects_in_lookup_table")),
        )

        iterate_distinct_project_list = rail.ForEachOperator(
            task_id='iterate_distinct_project_list',
            items=lambda: rail.result('load_all_distinct_projects_in_lookup_table'),
            start_task='if_first_is_present_and_last_is_not',
            end_task='iterate_distinct_project_list_end'
        )

        if_first_is_present_and_last_is_not = rail.IfOperator(
            task_id='if_first_is_present_and_last_is_not',
            test=python_callable.if_first_is_present_and_last_is_not,
            yes_task='add_item_to_final_data_1',
            no_task='if_first_and_last_is_not_present'
        )

        add_item_to_final_data_1 = rail.WriteLogOperator(
            task_id='add_item_to_final_data_1',
            log="{{ result('create_final_data_lookup_table') }}",
            message="na",
            severity="Success",
            properties={
                "Client": "{{ result('iterate_distinct_project_list').properties.client }}",
                "Project": "{{ result('iterate_distinct_project_list').properties.project }}",
                "Daterange": "{{ result('iterate_distinct_project_list').properties.timesheetperiod }}",
                "Perdiemtotals": "{{ result('iterate_distinct_project_list').properties.perdiem_amount }}",
                "Grossincometotals": "{{ result('iterate_distinct_project_list').properties['gross income'] }}",
                "Weeklytotals": "{{ result('iterate_distinct_project_list').properties.equipmentcost }}"
            }
        )

        if_first_and_last_is_not_present = rail.IfOperator(
            task_id='if_first_and_last_is_not_present',
            test=python_callable.if_first_and_last_is_not_present,
            yes_task='add_item_to_final_data_2',
            no_task='if_last_is_present_first_is_not'
        )

        add_item_to_final_data_2 = rail.WriteLogOperator(
            task_id='add_item_to_final_data_2',
            log="{{ result('create_final_data_lookup_table') }}",
            message="na",
            severity="Success",
            properties={
                "Client": None,
                "Project": None,
                "Daterange": "{{ result('iterate_distinct_project_list').properties.timesheetperiod }}",
                "Perdiemtotals": "{{ result('iterate_distinct_project_list').properties.perdiem_amount }}",
                "Grossincometotals": "{{ result('iterate_distinct_project_list').properties['gross income'] }}",
                "Weeklytotals": "{{ result('iterate_distinct_project_list').properties.equipmentcost }}"
            }
        )

        if_last_is_present_first_is_not = rail.IfOperator(
            task_id='if_last_is_present_first_is_not',
            test=python_callable.if_last_is_present_first_is_not,
            yes_task='add_item_to_final_data_3',
            no_task='if_first_and_last_is_present'
        )

        add_item_to_final_data_3 = rail.WriteLogOperator(
            task_id='add_item_to_final_data_3',
            log="{{ result('create_final_data_lookup_table') }}",
            message="na",
            severity="Success",
            properties={
                "Client": None,
                "Project": None,
                "Daterange": "{{ result('iterate_distinct_project_list').properties.timesheetperiod }}",
                "Perdiemtotals": "{{ result('iterate_distinct_project_list').properties.perdiem_amount }}",
                "Grossincometotals": "{{ result('iterate_distinct_project_list').properties['gross income'] }}",
                "Weeklytotals": "{{ result('iterate_distinct_project_list').properties.equipmentcost }}"
            }
        )

        add_item_to_final_data_summarize_1 = rail.WriteLogOperator(
            task_id='add_item_to_final_data_summarize_1',
            log="{{ result('create_final_data_lookup_table') }}",
            message="na",
            severity="Success",
            properties=lambda: python_callable.get_summarize_final_data(
                rail.result('foreach_distinct_projects_data_list').get('project'), config)
        )

        if_first_and_last_is_present = rail.IfOperator(
            task_id='if_first_and_last_is_present',
            test=python_callable.if_first_and_last_is_present,
            yes_task='add_item_to_final_data_4',
            no_task='iterate_distinct_project_list_end'
        )

        add_item_to_final_data_4 = rail.WriteLogOperator(
            task_id='add_item_to_final_data_4',
            log="{{ result('create_final_data_lookup_table') }}",
            message="na",
            severity="Success",
            properties={
                "Client": "{{ result('iterate_distinct_project_list').properties.client }}",
                "Project": "{{ result('iterate_distinct_project_list').properties.project }}",
                "Daterange": "{{ result('iterate_distinct_project_list').properties.timesheetperiod }}",
                "Perdiemtotals": "{{ result('iterate_distinct_project_list').properties.perdiem_amount }}",
                "Grossincometotals": "{{ result('iterate_distinct_project_list').properties['gross income'] }}",
                "Weeklytotals": "{{ result('iterate_distinct_project_list').properties.equipmentcost }}"
            }
        )

        add_item_to_final_data_summarize_2 = rail.WriteLogOperator(
            task_id='add_item_to_final_data_summarize_2',
            log="{{ result('create_final_data_lookup_table') }}",
            message="na",
            severity="Success",
            properties=lambda: python_callable.get_summarize_final_data(
                rail.result('foreach_distinct_projects_data_list').get('project'), config)
        )

        iterate_distinct_project_list_end = rail.EmptyOperator(
            task_id='iterate_distinct_project_list_end',
        )

        foreach_distinct_projects_data_list_end = rail.EmptyOperator(
            task_id='foreach_distinct_projects_data_list_end',
        )

        create_csv_format_final_data=rail.WriteCSVFileOperator(
            task_id='create_csv_format_final_data',
            source=lambda: rail.result('create_final_data_lookup_table'),
            header=[
                'Client Name',
                'Project Name',
                'Date Range',
                'Per Diem Totals',
                'Gross Income Totals',
                'Weekly Totals'
            ],
            row=request_payload.get_row_final_data
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_format_final_data')}}",
            output_file_name='{{dag_run_ecid()}}_{{current_time_in_specified_tz(fmt="%m%d%Y%H%M%S")}}' + "_projectcostingandperdiemextract.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_project_costing_and_perdiem_report_completed_mail = rail.EmailOperator(
            task_id='send_project_costing_and_perdiem_report_completed_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Project Costing & Perdiem Report completed {{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S") }}',
            html_content="templates/emails/send_project_costing_and_perdiem_report_completed_mail.html"
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        is_project_available >> rail.Label("Yes") >> is_daterange_available
        is_project_available >> rail.Label("No") >> send_no_project_selected_mail >> finish
        is_daterange_available >> rail.Label("Yes") >> is_daterange_does_not_contains_null
        is_daterange_available >> rail.Label("No") >> send_no_date_range_selected_mail >> finish
        is_daterange_does_not_contains_null >> rail.Label("Yes") >> get_start_end_date >> check_day_difference
        is_daterange_does_not_contains_null >> rail.Label("No") >> send_incomplete_date_range_mail >> finish
        check_day_difference >> rail.Label("Yes") >> send_end_date_is_less_than_start_date_mail >> finish
        check_day_difference >> rail.Label("No") >> get_file_format_script_uri >> create_payroll_download_batch
        create_payroll_download_batch >> execute_payroll_download_batch
        wait_for_payroll_download_batch >> get_payroll_download_batch_results >> download_payroll_file_from_url
        download_payroll_file_from_url >> load_payroll_file >> if_csv_has_no_data >> rail.Label("Yes") >> send_no_data_to_export_mail >> finish
        if_csv_has_no_data >> rail.Label("No") >> create_csv_format_payrolldata >> hourly_cost_report_for_project_costing >> if_uri_present_in_report_details
        if_uri_present_in_report_details >> rail.Label("Yes") >> get_entry_date_filter_and_project_filter_uri
        if_uri_present_in_report_details >> rail.Label("No") >> failed_dag_with_error >> finish
        get_entry_date_filter_and_project_filter_uri >> create_report_filter_1_list >> hourly_cost_report_entry
        hourly_cost_report_exit >> if_error_in_report_batch_result
        if_error_in_report_batch_result >> rail.Label("Yes") >> failed_dag_with_report_batch_result_error >> finish
        if_error_in_report_batch_result >> rail.Label("No") >> has_empty_report_data >> rail.Label("Yes") >> send_nodata_mail >> finish
        has_empty_report_data >> rail.Label("No") >> load_report_csv_file >> create_csv_format_hourly_cost_data >> expense_report_for_project_costing >> \
        if_uri_present_in_expense_report_details >> rail.Label("Yes") >> get_date_range_project_expense_type_filter_uri
        if_uri_present_in_expense_report_details >> rail.Label("No") >> failed_dag_with_uri_not_present >> finish
        get_date_range_project_expense_type_filter_uri >> get_all_expense_codes >> per_diem_expense_code_id >> add_value_to_filter_1_list >> \
        add_values_report_filter_2_list >> expense_report_entry
        expense_report_exit >> if_error_in_expense_report_batch_result
        if_error_in_expense_report_batch_result >> rail.Label("Yes") >> failed_dag_with_expense_report_batch_result_error >> finish
        if_error_in_expense_report_batch_result >> rail.Label("No") >> load_expense_report_csv_file >> create_collection_from_payroll_data_csv >>\
        create_collection_from_hourly_cost_data_csv >> query_data_list >> get_query_data >> if_query_list_data_records_rows_is_0 >> rail.Label("Yes") >> send_no_project_data_mail >> finish
        if_query_list_data_records_rows_is_0 >> rail.Label("No") >> if_query_list_data_records_rows_is_greater_than_0
        if_query_list_data_records_rows_is_greater_than_0 >> rail.Label("Yes") >> create_csv_format_expense_data >> create_collection_from_expense_data_csv >> project_costing_generate_report_lookup_table
        if_query_list_data_records_rows_is_greater_than_0 >> rail.Label("No") >> project_costing_generate_report_lookup_table
        project_costing_generate_report_lookup_table >> foreach_query_data_list_child >> wait_for_foreach_query_data_list_child >> if_entry_present
        if_entry_present >> rail.Label("Yes") >> query_distinct_projects_and_timesheets >> get_distinct_projects_and_timesheets_data >> accumulate_items_to_data_per_project_and_timesheet_list
        if_entry_present >> rail.Label("No") >> send_no_project_data_in_lookup_table_mail >> finish
        accumulate_items_to_data_per_project_and_timesheet_list >> foreach_accumulate_items_list >> if_project_present_in_accumulate_items
        if_project_present_in_accumulate_items >> rail.Label("Yes") >> add_success_entries_lookup_table >> foreach_accumulate_items_list_end
        if_project_present_in_accumulate_items >> rail.Label("No") >> foreach_accumulate_items_list_end
        foreach_accumulate_items_list >> foreach_accumulate_items_list_end >> query_distinct_projects >> get_distinct_projects_data
        get_distinct_projects_data >> create_final_data_lookup_table >> foreach_distinct_projects_data_list >> distinct_projects_in_lookup_table >> load_all_distinct_projects_in_lookup_table >> \
        iterate_distinct_project_list >> if_first_is_present_and_last_is_not
        if_first_is_present_and_last_is_not >> rail.Label("Yes") >> add_item_to_final_data_1 >> if_first_and_last_is_not_present
        if_first_is_present_and_last_is_not >> rail.Label("No") >> if_first_and_last_is_not_present
        if_first_and_last_is_not_present >> rail.Label("Yes") >> add_item_to_final_data_2 >> if_last_is_present_first_is_not
        if_first_and_last_is_not_present >> rail.Label("No") >> if_last_is_present_first_is_not
        if_last_is_present_first_is_not >> rail.Label("Yes") >> add_item_to_final_data_3 >> add_item_to_final_data_summarize_1 >> if_first_and_last_is_present
        if_last_is_present_first_is_not >> rail.Label("No") >> if_first_and_last_is_present
        if_first_and_last_is_present >> rail.Label("Yes") >> add_item_to_final_data_4 >> add_item_to_final_data_summarize_2 >> iterate_distinct_project_list_end
        if_first_and_last_is_present >> rail.Label("No") >> iterate_distinct_project_list_end
        iterate_distinct_project_list >> iterate_distinct_project_list_end >> foreach_distinct_projects_data_list_end
        foreach_distinct_projects_data_list >> foreach_distinct_projects_data_list_end >> create_csv_format_final_data >> generate_download_link >> \
        send_project_costing_and_perdiem_report_completed_mail >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
