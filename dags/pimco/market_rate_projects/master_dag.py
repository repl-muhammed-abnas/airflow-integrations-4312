from datetime import timedelta
from pendulum import datetime
import rail
from pimco.market_rate_projects.utils import request_payload
from pimco.market_rate_projects.utils import response_filter
from pimco.market_rate_projects.utils import python_callable_method

# pylint: disable=too-many-statements
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_market_rate_project_master_{config.instance}',
        description='PIMCO_Market_Rate_Project_Automation Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.master_dag_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        get_pimco_model_task_details = rail.RepliconServiceOperator(
            task_id="get_pimco_model_task_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjects2",
            data=request_payload.get_model_task
        )

        get_task_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_task_report_details',
            report_name=config.extract_task_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='model_project_task_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_task_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('model_project_task_report.get_report_result', 'has_data') }}",
            yes_task='load_task_report_data',
            no_task='fail_no_report_data',
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="Report \"Model project - task details\" execution failed",
        )

        load_task_report_data = rail.LoadCSVFileOperator(
            task_id='load_task_report_data',
            document="{{ result('model_project_task_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_project_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_project_report_details',
            report_name=config.extract_project_report_name,
        )

        report_task_group_entry, report_task_group_exit = rail.run_report(
            group_id='project_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_project_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        project_report_has_data = rail.IfOperator(
            task_id="project_report_has_data",
            test="{{ result('project_report.get_report_result', 'has_data') }}",
            yes_task='load_project_report_data',
            no_task='fail_no_task_report_data',
        )

        fail_no_task_report_data = rail.FailOperator(
            task_id="fail_no_task_report_data",
            message="Report \"**In-progress Project details**\" execution failed",
        )

        load_project_report_data = rail.LoadCSVFileOperator(
            task_id='load_project_report_data',
            document="{{ result('project_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        project_report_data_collection = rail.CreateCollectionOperator(
            task_id='project_report_data_collection',
            source="{{ result('load_project_report_data') }}",
            name='projectreportdata',
            columns={
                'Fund/Deal/Entity Name': 'projectname',
                'Fund/Deal/Entity Code': 'projectcode',
                'Project URI': 'projecturi',
            }
        )

        query_all_project_data = rail.QueryCollectionOperator(
            task_id='query_all_project_data',
            query="""SELECT * FROM projectreportdata"""
        )

        task_report_data_collection = rail.CreateCollectionOperator(
            task_id='task_report_data_collection',
            source="{{ result('load_task_report_data') }}",
            name='taskreportdata',
            columns={
                'Billing Activity-Task Name (Full Path)': 'taskfullpath',
                'Billing Activity-Task Description': 'taskdescription',
                'Billing Activity-Task Status': 'taskstatus',
                'Created': 'created',
                'Billing Activity-Task Code': 'taskcode',
                'Billing Activity-Task Market Rate': 'marketrate',
                'Org Costs': 'orgcosts',
                'Lux Flag': 'luxflag',
                'taskUri': 'taskuri',
                'Billing Activity-Task Start Date': 'taskstartdate',
                'Billing Activity-Task End Date': 'taskenddate',
                'Billing Activity-Task Name': 'taskname',
                'Billing Activity-Task Time & Expense Entry Type': 'entrytype',
                'Billing Activity-Task Cost Type': 'costtype',
                'Billing Activity-Task Estimated Hours': 'estimatedhrs',
                'Updated': 'updated'
            }
        )

        query_task_to_be_updated = rail.QueryCollectionOperator(
            task_id='query_task_to_be_updated',
            query="""SELECT * FROM taskreportdata where updated = 'Yes' """
        )

        has_task_to_be_updated = rail.IfOperator(
            task_id='has_task_to_be_updated',
            test="{{ result('query_task_to_be_updated','length') > 0 }}",
            yes_task='query_distinct_projects',
            no_task='send_no_updation_mail'
        )

        send_no_updation_mail = rail.EmailOperator(
            task_id='send_no_updation_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Updating market rate from base project to all projects completed successfully without any market rate to be updated at - {{ current_time_in_specified_tz("America/Los_Angeles") }}',
            html_content="templates/emails/no_updation_mail.html"
        )

        query_distinct_projects = rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            query="""SELECT projectname, projecturi FROM projectreportdata"""
        )

        get_custom_field_list = rail.RepliconServiceOperator(
            task_id="get_custom_field_list",
            endpoint="/services/TaskCustomFieldListService1.svc/GetData",
            data=request_payload.get_custom_field_list,
            response_filter=response_filter.get_filtered_custom_field_list
        )

        get_all_currencies = rail.RepliconServiceOperator(
            task_id="get_all_currencies",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
        )

        process_market_rate_update_child = rail.trigger_parallel_dagrun(
            task_id='process_market_rate_update_child',
            items="{{ result('query_distinct_projects') }}",
            parallel_count=config.process_market_rate_update_child_count,
            trigger_dag_id=f'pimco_market_rate_project_child_{config.instance}',
            conf=request_payload.process_market_rate_update_child_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_task_heirarchy = rail.PythonOperator(
            task_id='get_task_heirarchy',
            python_callable=python_callable_method.get_task_heirarchy
        )

        update_custom_field = rail.RepliconServiceOperator(
            task_id="update_custom_field",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.update_custom_field,
        )

        is_update_custom_field_successfull = rail.IfOperator(
            task_id='is_update_custom_field_successfull',
            test="{{result('update_custom_field') | filter_by_attr('error', 'does-not-equal', None)|is_falsy}}",
            yes_task='filter_master_log',
            no_task='log_update_custom_field_failed'
        )

        log_update_custom_field_failed = rail.WriteLogOperator(
            task_id='log_update_custom_field_failed',
            message="Update Custom Field Task was Unsuccessfull",
            severity='Error',
            properties={
                'projectname': "{{ result('get_pimco_model_task_details').0.name }}",
                'runid': "{{ dag_run.run_id }}",
                'status': "Error",
            }
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Updating market rate from base project to all projects completed successfully at - {{ current_time_in_specified_tz("America/Los_Angeles") }}',
            html_content="templates/emails/completion_mail.html"
        )


        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Error',
        )

        is_all_child_successfull = rail.IfOperator(
            task_id='is_all_child_successfull',
            test="{{ result('filter_master_log', 'length') > 0 }}",
            yes_task='render_logs_csv',
            no_task='send_completion_mail'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['Projectname', 'Dagrunid', 'Status', 'Details', 'Ecid', '{{ current_time("%d/%m/%YT%H:%M:%S") }}'],
            row=['{{ item.properties.projectname }}', '{{ item.properties.runid }}',
                 '{{ item.properties.status }}', '{{ item.message }}', '{{ item.ecid }}'],
        )

        open_brackets = "{{"
        close_brackets = "}}"

        send_child_dags_failure_mail = rail.EmailOperator(
            task_id='send_child_dags_failure_mail',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Updating market rate from base project to all projects Failed at - {{ current_time_in_specified_tz("America/Los_Angeles") }}',
            html_content="templates/emails/child_failure_mail.html",
            files=[(f'{open_brackets}get_company_key(){close_brackets}_market_rate_update_log_file_{open_brackets}current_time(){close_brackets}.csv',
                    f"{open_brackets}result('render_logs_csv'){close_brackets}")],
            params={
                'emails': config.tenant_email
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        send_failure_mail = rail.EmailOperator(
            task_id='send_failure_mail',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Updating market rate from base project to all projects Failed at - {{ current_time_in_specified_tz("America/Los_Angeles") }}',
            html_content="templates/emails/failure_mail.html",
            params={
                'dag_id': f'pimco_market_rate_project_master_{config.instance}'
            }
        )

        get_pimco_model_task_details >> get_task_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "Yes") >> load_task_report_data
        report_has_data >> rail.Label('No') >> fail_no_report_data
        load_task_report_data >> get_project_report_details >> report_task_group_entry
        report_task_group_exit >> project_report_has_data >> rail.Label(
            "Yes") >> load_project_report_data >> project_report_data_collection
        project_report_has_data >> rail.Label('No') >> fail_no_task_report_data
        project_report_data_collection >> query_all_project_data >> task_report_data_collection >> query_task_to_be_updated
        query_task_to_be_updated >> has_task_to_be_updated >> rail.Label(
            'No') >> send_no_updation_mail
        has_task_to_be_updated >> rail.Label(
            'Yes') >> query_distinct_projects >> get_custom_field_list >> get_all_currencies
        get_all_currencies >> process_market_rate_update_child >> get_task_heirarchy >> update_custom_field
        update_custom_field >> is_update_custom_field_successfull >> rail.Label('Yes') >> filter_master_log
        filter_master_log >> is_all_child_successfull
        update_custom_field >> is_update_custom_field_successfull >> rail.Label('No') >> log_update_custom_field_failed >> filter_master_log
        is_all_child_successfull >> rail.Label('Yes') >> send_completion_mail >> on_error >> send_failure_mail
        is_all_child_successfull >> rail.Label('No') >> render_logs_csv >> send_child_dags_failure_mail >> on_error

    return dag


rail.for_each_instance(create_main_airflow_dag)
