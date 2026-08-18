import rail
from cie_randstadlifescience.expenseDataExport import payloads
from cie_randstadlifescience.expenseDataExport.utils import data_formatting


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}process_expense_uris_chunk_wisechild_dag{dag_id_postfix}',
        description=f'{dag_id_prefix}process_expense_uris_chunk_wise_child_dag{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_bulk_expense_details = rail.RepliconServiceOperator(
            task_id="get_bulk_expense_details",
            endpoint="/services/ExpenseService1.svc/BulkGetExpenseSheetDetails",
            data=payloads.get_expense_detail_payload,
        )

        add_expenses_to_variable = rail.PythonOperator(
            task_id='add_expenses_to_variable',
            python_callable=data_formatting.add_expenses_to_variable,
            op_args=[
                '{{ result("get_bulk_expense_details") | tojson }}', config]
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_bulk_expense_details >> add_expenses_to_variable >> finish

    return dag


rail.for_each_instance(create_child_dag_wbs)
