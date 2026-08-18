# pylint: disable=too-many-statements
from datetime import datetime
import rail
from caretakers.project_invoice_report_extract_on_demand.utils.python_callable import get_today
from caretakers.project_invoice_report_extract_on_demand.utils import python_callable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'caretakers_project_invoice_report_export_master_{config.instance}',
        description=f'caretakers_project_invoice_report_export_master_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_date_range_data = rail.PythonOperator(
            task_id="get_date_range_data",
            python_callable=python_callable.get_daterange_data
        )

        is_date_range_less_than_0 = rail.IfOperator(
            task_id="is_date_range_less_than_0",
            test="{{ result('get_date_range_data').daterange_diff < 0}}",
            yes_task="send_end_date_before_start_date_mail",
            no_task="get_report_uri"
        )

        get_report_uri = rail.RepliconServiceOperator(
            task_id="get_report_uri",
            endpoint="/services/reportservice1.svc/GetAllReports",
            data_handler=lambda response:
            {
                "project_report_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Project Budget Balance Summary_Integration', 'uri', ''),
                "not_invoiced_report_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 
                                                                                 'Not Invoiced Amount ( all projects)_ Integrations', 'uri', '')
            }
        )

        get_project_report_data_uri = rail.RepliconServiceOperator(
            task_id="get_project_report_data_uri",
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result('get_report_uri')['project_report_uri']
            },
            data_handler=lambda response:{
                "creationdatefilter_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'CreationDateFilter', 'uri', '')
            }
        )

        get_not_invoiced_report_data_uri = rail.RepliconServiceOperator(
            task_id="get_not_invoiced_report_data_uri",
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result('get_report_uri')['not_invoiced_report_uri']
            },
            data_handler=lambda response:{
                "daterangefilter_uri" : rail.find_first_by_attr_and_get_attr(
                    response['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri', '')
            }
        )

        add_entry_dates_to_lists = rail.PythonOperator(
            task_id="add_entry_dates_to_lists",
            python_callable=python_callable.add_entry_dates
        )

        run_project_report_entry, run_project_report_exit = rail.run_report(
            group_id='run_report_project',
            report_params=python_callable.get_project_report_params
        )

        is_project_report_failed = rail.IfOperator(
            task_id="is_project_report_failed",
            test='{{result("run_report_project.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_project_report_generation",
            no_task="load_project_report_data"
        )

        fail_project_report_generation = rail.FailOperator(
            task_id="fail_project_report_generation",
            message="{{result('run_report_project.get_report_result').reportGenerationResults[0].error}}"
        )

        load_project_report_data = rail.LoadCSVFileOperator(
            task_id='load_project_report_data',
            document="{{ result('run_report_project.get_report_result').reportGenerationResults[0].payload }}",
        )

        run_notinvoiced_report_entry, run_notinvoiced_report_exit = rail.run_report(
            group_id='run_report_notinvoiced',
            report_params=python_callable.get_notinvoiced_report_params
        )

        is_notinvoiced_report_failed = rail.IfOperator(
            task_id="is_notinvoiced_report_failed",
            test='{{result("run_report_notinvoiced.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_notinvoiced_report_generation",
            no_task="load_notinvoiced_report_data"
        )

        fail_notinvoiced_report_generation = rail.FailOperator(
            task_id="fail_notinvoiced_report_generation",
            message="{{result('run_report_notinvoiced.get_report_result').reportGenerationResults[0].error}}"
        )

        load_notinvoiced_report_data = rail.LoadCSVFileOperator(
            task_id='load_notinvoiced_report_data',
            document="{{ result('run_report_notinvoiced.get_report_result').reportGenerationResults[0].payload }}",
        )

        project_report_data_collection = rail.CreateCollectionOperator(
            task_id="project_report_data_collection",
            source="{{ result('load_project_report_data') }}",
            columns={
                "Client Name": "client",
                "Project Name": "project",
                "Project Manager": "manager",
                "Total Budget": "totalbudget",
                "Total Invoice amount": "totalinvoiceamount",
                "Remaining Budget": "remainingbudget",
                "projecturi": "projecturi"
            },
            name="projectinput"
        )

        query_from_project_input = rail.QueryCollectionOperator(
            task_id="query_from_project_input",
            query="""SELECT * FROM projectinput WHERE NULLIF(projecturi, '') IS NOT NULL """,
            name="project_input_data"
        )

        notinvoiced_report_data_collection = rail.CreateCollectionOperator(
            task_id="notinvoiced_report_data_collection",
            source="{{ result('load_notinvoiced_report_data') }}",
            columns={
                "Client Name": "client",
                "Project Name": "project",
                "Project Manager": "manager",
                "Not Invoiced Amount (Selected Dates)(BC)": "notinvoicedamount",
                "Total Budget": "totalbudget",
                "projecturi": "projecturi"
            },
            name="notinvoicedinput"
        )

        query_from_notinvoiced_input = rail.QueryCollectionOperator(
            task_id="query_from_notinvoiced_input",
            query="""SELECT * FROM notinvoicedinput WHERE NULLIF(projecturi, '') IS NOT NULL """,
            name="notinvoiced_input_data"
        )

        is_project_data_present = rail.IfOperator(
            task_id="is_project_data_present",
            test='{{result("query_from_project_input", "length") > 0 }}',
            yes_task="add_project_data_to_final_list",
            no_task="query_distinct_not_invoiced"
        )

        add_project_data_to_final_list = rail.PythonOperator(
            task_id = "add_project_data_to_final_list",
            python_callable=python_callable.add_project_data
        )

        query_distinct_not_invoiced = rail.QueryCollectionOperator(
            task_id="query_distinct_not_invoiced",
            query="""SELECT * FROM notinvoiced_input_data WHERE projecturi NOT IN (SELECT DISTINCT projecturi FROM project_input_data) """,
            name="filtered_notinvoiced_data"
        )

        is_distinct_notinvoiced_data_present = rail.IfOperator(
            task_id="is_distinct_notinvoiced_data_present",
            test='{{result("query_distinct_not_invoiced", "length") > 0 }}',
            yes_task="add_not_invoiced_data_to_final_list",
            no_task="is_final_list_data_present"
        )

        add_not_invoiced_data_to_final_list = rail.PythonOperator(
            task_id = "add_not_invoiced_data_to_final_list",
            python_callable=python_callable.add_notinvoiced_data
        )

        is_final_list_data_present = rail.IfOperator(
            task_id="is_final_list_data_present",
            test='{{ result("query_from_project_input", "length") > 0 }}',
            yes_task="get_final_list",
            no_task="send_no_data_for_daterange_email"
        )

        get_final_list = rail.PythonOperator(
            task_id = "get_final_list",
            python_callable=python_callable.get_final_list_data
        )

        compose_csv_with_header = rail.WriteCSVFileOperator(
            task_id="compose_csv_with_header",
            source=lambda: rail.result('get_final_list'),
            header=[
                "Client Name",
                "Project Name",
                "Project Manager",
                "Total Budget",
                "Total Invoice Amount (BC)",
                "Not Invoiced Amount (Selected Dates)(BC)",
                "Remaining Budget"
            ],
            row=python_callable.get_rows
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda : 'Repliconinvoice_' + datetime.today().strftime('%m%d%YT%H%M') + '.csv'
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
            subject=f'{config.company_key} | Custom invoice report export processed - ' + get_today(),
            html_content="templates/email/cshare_email.html"
        )

        send_error_mail = rail.EmailOperator(
            task_id='send_error_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom invoice report not processed - ' + get_today(),
            html_content="templates/email/error_email.html"
        )

        send_end_date_before_start_date_mail = rail.EmailOperator(
            task_id='send_end_date_before_start_date_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom invoice report not processed - ' + get_today(),
            html_content="templates/email/end_date_before_start_date_email.html"
        )

        send_no_data_for_daterange_email = rail.EmailOperator(
            task_id='send_no_data_for_daterange_email',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | Custom invoice report not processed - ' + get_today(),
            html_content="templates/email/no_data_for_given_daterange_email.html"
        )


        get_date_range_data >> is_date_range_less_than_0 >> rail.Label("No") >> get_report_uri

        is_date_range_less_than_0 >> rail.Label("Yes") >> send_end_date_before_start_date_mail

        get_report_uri >> get_project_report_data_uri >> get_not_invoiced_report_data_uri >> add_entry_dates_to_lists >> \
        run_project_report_entry

        run_project_report_exit >> is_project_report_failed >> rail.Label("Yes") >> fail_project_report_generation

        is_project_report_failed >> rail.Label("No") >> load_project_report_data >> run_notinvoiced_report_entry

        run_notinvoiced_report_exit >> is_notinvoiced_report_failed >> rail.Label("Yes") >> fail_notinvoiced_report_generation

        is_notinvoiced_report_failed >> rail.Label("No") >> load_notinvoiced_report_data >> project_report_data_collection >> \
        query_from_project_input >> notinvoiced_report_data_collection >> query_from_notinvoiced_input >> is_project_data_present

        is_project_data_present >> rail.Label("No") >> query_distinct_not_invoiced

        is_project_data_present >> rail.Label("Yes") >> add_project_data_to_final_list >> query_distinct_not_invoiced

        query_distinct_not_invoiced >> is_distinct_notinvoiced_data_present

        is_distinct_notinvoiced_data_present >> rail.Label("Yes") >> add_not_invoiced_data_to_final_list >> get_final_list

        is_distinct_notinvoiced_data_present >> rail.Label("No") >> is_final_list_data_present

        is_final_list_data_present >> rail.Label("No") >> send_no_data_for_daterange_email

        is_final_list_data_present >> rail.Label("Yes") >> get_final_list

        get_final_list >> compose_csv_with_header >> get_file_name >> filter_master_log >> \
        any_records_failed >> rail.Label("Yes") >> send_error_mail >> log_to_sumo

        any_records_failed >> rail.Label("No") >> generate_download_link >> send_mail_with_cshare >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
