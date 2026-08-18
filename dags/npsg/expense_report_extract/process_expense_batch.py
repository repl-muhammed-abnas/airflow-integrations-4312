import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'npsg_expense_report_extract_process_expense_sheet_reimbursement_child_{config.instance}',
        description=f'NPSG_expense_report_extract {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=5,

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_expense_sheet_reimbursement_batch = rail.RepliconServiceOperator(
            task_id='create_expense_sheet_reimbursement_batch',
            endpoint="/services/ExpenseService1.svc/CreateExpenseSheetReimbursementBatch",
            data=lambda dag_run: {
                "expenseUris": dag_run.conf['items'],
                "expenseReimbursementOptionUri": "urn:replicon:expense-reimbursement-option:reimburse-employee"
            }
        )

        execute_expense_reimbursement_batch = rail.RepliconServiceOperator(
            task_id='execute_expense_reimbursement_batch',
            endpoint="/services/ExpenseService1.svc/ExecuteExpenseReimbursementBatch",
            data={
                "expenseReimbursementBatchUri": "{{ result('create_expense_sheet_reimbursement_batch') }}"
            }
        )

        check_for_execution_error = rail.IfOperator(
            task_id = "check_for_execution_error",
            test="{{ result('execute_expense_reimbursement_batch').errors | is_truthy}}",
            yes_task="fail_dag_due_to_batch_execution_failure"
        )


        fail_dag_due_to_batch_execution_failure = rail.PythonOperator(
            task_id = "fail_dag_due_to_batch_execution_failure",
            python_callable = lambda: "Failing dag_run as Expense Reimbursement Batch is completed with error",
        )

        create_expense_sheet_reimbursement_batch >> execute_expense_reimbursement_batch >> check_for_execution_error >>\
            rail.Label("has errors") >> fail_dag_due_to_batch_execution_failure

        return dag


rail.for_each_instance(create_dag)
