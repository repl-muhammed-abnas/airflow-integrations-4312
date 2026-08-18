
from datetime import timedelta
from airflow.models import Variable
from isuzu.invoice_sync.utils import python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_get_the_total_invoiced_based_on_projects_and_update_the_custom_field_child_{config.instance}',
        description=f'Get the total invoiced data based on projects and update the required custom field {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_report_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_project_report_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_project_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_project_report_details',
            report_name=config.report2_name
        )
        project_report_data_generation = rail.run_report2(
            group_id='project_report_data_generation',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_project_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv_7_7_7 = rail.LoadCSVFileOperator(
            task_id="parse_csv_7_7_7",
            document="{{ result('project_report_data_generation.get_report_result').reportGenerationResults[0].payload }}",
        )
        load_csv_data = rail.PythonOperator(
            task_id='load_csv_data',
            python_callable=python_callable.get_project_report_data
        )

        accumulate_list_items_17_17_8 = rail.SetVariableOperator(
            task_id='accumulate_list_items_17_17_8',
            name='Reportdata',
            append=True,
            value={
                "invoicestatus": "{{ result('load_csv_data')[0].get('Invoice Status') }}",
                "client_name": "{{ result('load_csv_data')[0].get('Client Name') }}",
                "project_name": "{{ result('load_csv_data')[0].get('Project Name') }}",
                "invoicelineitemamount": "{{ result('load_csv_data')[0].get('Invoice Line Item Amount (BC)') }}",
                "projecturi": "{{ result('load_csv_data')[0].get('Project URI') }}"
            }
        )
        declare_accumulate_list_items = rail.SetVariableOperator(
            task_id='declare_accumulate_list_items',
            name='Updated Projects',
            append=False,
            value=[]
        )
        foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9 = rail.ForEachOperator(
            task_id='foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9',
            items=lambda: rail.result('load_csv_data'),
            start_task='if_foreach_parse_csv_9_parse_csv_7_9_column_4_present_10',
            end_task='foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9_end'
        )

        if_foreach_parse_csv_9_parse_csv_7_9_column_4_present_10 = rail.IfOperator(
            task_id='if_foreach_parse_csv_9_parse_csv_7_9_column_4_present_10',
            test='''{{ result('foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9').get('Project URI') | is_truthy  and result('foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9').get('Invoice Line Item Amount (BC)') | is_truthy }}''',
            yes_task="log_11",
            no_task="foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9_end",
        )

        log_11 = rail.PythonOperator(
            task_id='log_11',
            python_callable=lambda: rail.result('foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9').get(
                'Invoice Line Item Amount (BC)').split('$')[1]
        )

        if_log_2_contains_12 = rail.IfOperator(
            task_id='if_log_2_contains_12',
            test='''{{ result('log_11') | matches(',') }}''',
            yes_task="log_13",
            no_task="updatethe_invoice_amount1customfieldonproject_16",
        )

        log_13 = rail.PythonOperator(
            task_id='log_13',
            python_callable=lambda: "".join(rail.result('log_11').split(','))
        )

        updatethe_invoice_amount1customfieldonproject_14 = rail.RepliconServiceOperator(
            task_id='updatethe_invoice_amount1customfieldonproject_14',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda: {
                "objectUri": rail.result('foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9').get('Project URI'),
                "customFieldUri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":user-defined-field:cae598fe-010b-42ff-bacd-e957f12cfdc8",
                "value": rail.result('log_13')
            }
        )

        updatethe_invoice_amount1customfieldonproject_16 = rail.RepliconServiceOperator(
            task_id='updatethe_invoice_amount1customfieldonproject_16',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda: {
                "objectUri": rail.result('foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9').get('Project URI'),
                "customFieldUri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":user-defined-field:cae598fe-010b-42ff-bacd-e957f12cfdc8",
                "value": rail.result('log_11')
            }
        )

        accumulate_list_items_17_17_17 = rail.SetVariableOperator(
            task_id='accumulate_list_items_17_17_17',
            name='Updated Projects',
            append=True,
            value={
                "project_name": "{{ result('foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9').get('Project Name') }}"
            }
        )

        foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9_end = rail.EmptyOperator(
            task_id='foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9_end',
        )
        get_variable_data = rail.GetVariableOperator(
            task_id='get_variable_data',
            name='{{ result("declare_accumulate_list_items").name }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_project_report_details
        get_project_report_details >> project_report_data_generation >> parse_csv_7_7_7 >> load_csv_data >> accumulate_list_items_17_17_8 >> declare_accumulate_list_items >> foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9 >> if_foreach_parse_csv_9_parse_csv_7_9_column_4_present_10
        if_foreach_parse_csv_9_parse_csv_7_9_column_4_present_10 >> rail.Label(
            'Yes') >> log_11 >> if_log_2_contains_12
        if_log_2_contains_12 >> rail.Label(
            'Yes') >> log_13 >> updatethe_invoice_amount1customfieldonproject_14 >> accumulate_list_items_17_17_17 >> foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9_end
        if_log_2_contains_12 >> rail.Label(
            'No') >> updatethe_invoice_amount1customfieldonproject_16 >> accumulate_list_items_17_17_17
        if_foreach_parse_csv_9_parse_csv_7_9_column_4_present_10 >> rail.Label(
            'No') >> foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9_end
        foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9 >> foreach_parse_csv_9_parse_csv_7_9_parse_csv_7_7_9_end >> get_variable_data >> finish

    return dag


rail.for_each_instance(create_dag)
