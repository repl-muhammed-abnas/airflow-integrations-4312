from datetime import timedelta, datetime
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'solver_expense_report_extract_friday_run_master_{config.instance}',
        description=f'SolverInc_Flag_Expenses_with_incorrect_incurred_date_Friday_run  {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval_friday,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_expense_incurred_report_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_expense_incurred_report_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_expense_incurred_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_expense_incurred_report_details',
            report_name=config.expense_incurred_report
        )

        run_expense_incurred_report=rail.run_report2(
            group_id='run_expense_incurred_report',
           report_params= lambda: {
                #pylint: disable = line-too-long
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_expense_incurred_report_details')['uri'],
                        "filterValues": [
                            {
                            "reportFilterUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:report-filter:a9109a8c211443b1ae46036648533f61;daterangefilter_incurreddate",
                            "value": "ThisWeek"
                            },
                            {
                            "reportFilterUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:report-filter:a9109a8c211443b1ae46036648533f61;daterangefilter_incurreddate",
                            "value": null
                            },
                            {
                            "reportFilterUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:report-filter:a9109a8c211443b1ae46036648533f61;daterangefilter_incurreddate",
                            "value": null
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv_report_result=rail.LoadCSVFileOperator(
            task_id='parse_csv_report_result',
            document="{{result('run_expense_incurred_report.get_report_result').reportGenerationResults[0].payload}}",
            delimiter=','
        )

        def create_expensedata_list():
            expense_data = rail.load_all_records(rail.result('parse_csv_report_result'))
            return [ {
                'username': data['User Name'],
                'trackingnumber': data['Tracking Number'],
                'amountcurrency': data['Amount - Currency'],
                'amountamount': data['Amount - Amount'],
                'incurreddate': data['Incurred Date'],
                'savedon': data['Saved On'],
                'weekstartdatesavedon': data['Week Start Date (Saved On)'],
                'weekenddatesavedon': data['Week End Date (Saved On)'],
                'usersupervisor': data['User Supervisor Name (Current)'],
                'usersupervisoremailaddress': data['User Supervisor Email address'],
                'daydiff': (datetime.strptime(data['Incurred Date'],'%b %d, %Y') - datetime.strptime(data['Week Start Date (Saved On)'],'%b %d, %Y')).days
            } for data in expense_data]

        create_expense_data_list = rail.PythonOperator(
            task_id = 'create_expense_data_list',
            python_callable= create_expensedata_list
        )

        if_expensedata_list_has_records=rail.IfOperator(
            task_id='if_expensedata_list_has_records',
            test=lambda: bool(rail.result('create_expense_data_list')),
            yes_task="create_exepensedata_collection",
            no_task="finish",
        )

        create_exepensedata_collection = rail.CreateCollectionOperator(
            task_id='create_exepensedata_collection',
            source = lambda: rail.result('create_expense_data_list'),
            name = "expensedata",
        )

        query_unique_supervisor=rail.QueryCollectionOperator(
            task_id='query_unique_supervisor',
            query="""SELECT DISTINCT expensedata.usersupervisoremailaddress FROM expensedata""",
        )

        trigger_dag_to_send_mail_per_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id = 'trigger_dag_to_send_mail_per_supervisor',
            retries = 0,
            items="{{ result('query_unique_supervisor') }}",
            trigger_dag_id=f'solver_expense_report_extract_send_mail_per_supervisor_friday_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "usersupervisoremailaddress": item['usersupervisoremailaddress']
            }
        )

        wait_for_child_dag = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_to_send_mail_per_supervisor") }}'
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_expense_incurred_report_details >> run_expense_incurred_report
        run_expense_incurred_report >> parse_csv_report_result >> create_expense_data_list >> if_expensedata_list_has_records
        if_expensedata_list_has_records >> rail.Label('Yes') >> create_exepensedata_collection >> query_unique_supervisor
        query_unique_supervisor >> trigger_dag_to_send_mail_per_supervisor
        trigger_dag_to_send_mail_per_supervisor >> wait_for_child_dag >> finish
        if_expensedata_list_has_records >> rail.Label('No') >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
