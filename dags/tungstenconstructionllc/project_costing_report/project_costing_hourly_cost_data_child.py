# pylint: disable=too-many-statements unnecessary-lambda
from tungstenconstructionllc.project_costing_report.utils import python_callable
import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'tungstenconstructionllc_project_costing_hourly_cost_report_child_{config.instance}',
        description=f'{config.company_key} Project Costing Hourly Cost Report Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        query_hourly_cost_data= rail.QueryCollectionOperator(
            task_id= 'query_hourly_cost_data',
            query="""SELECT * FROM hourly_cost_data WHERE loginname == :login_name AND project == :project_name
                AND timesheetstartdate == :timsheet_start AND timesheetenddate == :timesheet_end""",
            query_params={
                "login_name": "{{ dag_run.conf.loginname }}",
                "project_name": "{{dag_run.conf.projectname}}",
                "timsheet_start": "{{dag_run.conf.timsheetstart}}",
                "timesheet_end": "{{dag_run.conf.timesheetend}}"
            },
            name= 'hourlycostrawdata'
        )

        parse_csv_has_data =rail.IfOperator(
            task_id='parse_csv_has_data',
            test="{{ dag_run.conf.get('parse_csv') | load_all_records() | length > 0 }}",
            yes_task="query_per_diem_expense_data",
            no_task="has_query_data",
        )

        query_per_diem_expense_data= rail.QueryCollectionOperator(
            task_id= 'query_per_diem_expense_data',
            query="""SELECT clientname,projectname,username,loginname,expensecode,trackingnumber,incurreddate,SUM(amount) as total_amount,
                    approvalstatus FROM expense_data WHERE incurreddate BETWEEN :timsheet_start AND :timesheet_end AND loginname == :login_name
                    AND projectname == :project_name AND expensecode='Per Diem'""",
            query_params={
                "login_name": "{{ dag_run.conf.loginname }}",
                "project_name": "{{dag_run.conf.projectname}}",
                "timsheet_start": "{{dag_run.conf.timsheetstart}}",
                "timesheet_end": "{{dag_run.conf.timesheetend}}"
            },
            name= 'perdiemexpenserawdata'
        )

        has_query_data =rail.IfOperator(
            task_id='has_query_data',
            test=lambda:len(rail.result('query_hourly_cost_data')) > 0 or len(rail.result('query_per_diem_expense_data')) > 0,
            yes_task="log_success_entries",
            no_task="finish",
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="Success",
            properties=lambda dag_run: python_callable.get_log_success_properties(dag_run)
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        query_hourly_cost_data >> parse_csv_has_data >> rail.Label("Yes") >> query_per_diem_expense_data >> has_query_data
        parse_csv_has_data >> rail.Label("No") >> has_query_data >> rail.Label("Yes") >> log_success_entries >> finish
        has_query_data >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_child_dag)
