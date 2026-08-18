from datetime import timedelta
from airflow.models import Variable
import rail
import uuid
from lead3rllc.expense_import.utils.request_payload import put_expense_sheet_payload_invoice


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_create_expense_sheet_for_invoice_dag_id,
        description=f'LEAD3R LLC Expense Import Invoice - Create Expense Sheet {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_get_matching_records_from_valid_records_for_expense_sheet'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_get_matching_records_from_valid_records_for_expense_sheet',
            end_task='catch_and_log_error',
        )

        query_get_matching_records_from_valid_records_for_expense_sheet = rail.QueryCollectionOperator(
            task_id='query_get_matching_records_from_valid_records_for_expense_sheet',
            query='''SELECT * FROM valid_records_to_process WHERE
                    Vendor_Name = :vendor_name
                    AND Invoice_Date = :invoice_date''',
            query_params={
                'vendor_name': '{{ dag_run.conf.vendor_name }}',
                'invoice_date': '{{ dag_run.conf.invoice_date }}'
            }
        )

        create_new_expense_sheet_draft = rail.RepliconServiceOperator(
            task_id='create_new_expense_sheet_draft',
            endpoint="/services/ExpenseService1.svc/CreateNewExpenseSheetDraft",
            data=lambda dag_run: {
                "ownerUri": dag_run.conf['owner_uri']
            }
        )

        update_expense_sheet_description = rail.RepliconServiceOperator(
            task_id='update_expense_sheet_description',
            endpoint="/services/ExpenseService1.svc/UpdateExpenseSheetDescription",
            data=lambda dag_run: {
                "expenseSheetUri": rail.result('create_new_expense_sheet_draft'),
                "description": dag_run.conf['vendor_name']
            }
        )

        publish_expense_sheet_draft = rail.RepliconServiceOperator(
            task_id='publish_expense_sheet_draft',
            endpoint="/services/ExpenseService1.svc/PublishExpenseSheetDraft",
            data={
                "draftUri": "{{ result('create_new_expense_sheet_draft') }}"
            }
        )

        put_expense_sheet_entries = rail.RepliconServiceOperator(
            task_id='put_expense_sheet_entries',
            endpoint="/services/ExpenseService1.svc/PutExpenseSheet",
            data=put_expense_sheet_payload_invoice
        )

        submit_expense_sheet = rail.RepliconServiceOperator(
            task_id='submit_expense_sheet',
            endpoint="/services/ExpenseApprovalService1.svc/Submit",
            data=lambda: {
                "expenseUri": rail.result('publish_expense_sheet_draft')['uri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Submitted by Integration"
            }
        )

        add_expense_sheet_success_log_entry = rail.WriteLogOperator(
            task_id='add_expense_sheet_success_log_entry',
            log="{{dag_run.conf.expense_invoice_import_logs}}",
            message='na',
            severity='Success',
            properties=lambda dag_run: {
                "vendor_name": dag_run.conf['vendor_name'],
                "invoice_date": dag_run.conf['invoice_date'],
                "line_description": '',
                "expense_type": '',
                "project": '',
                "action": "Create Expense Sheet",
                "status": "Success",
                "details": "Expense sheet created successfully"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.expense_invoice_import_logs}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "vendor_name": dag_run.conf['vendor_name'],
                "invoice_date": dag_run.conf['invoice_date'],
                "line_description": '',
                "expense_type": '',
                "project": '',
                "action": "Create Expense Sheet",
                "status": 'Error',
                "details": ("Error in creating expense sheet as one or more line items have invalid data :" if rail.result(
                    'publish_expense_sheet_draft') else "Error in creating expense sheet ;") + rail.render_template("{{get_error_message()}}")
            }
        )

        if_error_in_line_items = rail.IfOperator(
            task_id='if_error_in_line_items',
            trigger_rule='all_success',
            test="{{ get_task_state('put_expense_sheet_entries') == 'failed' }}",
            yes_task='delete_expense_sheet'
        )

        delete_expense_sheet = rail.RepliconServiceOperator(
            task_id='delete_expense_sheet',
            endpoint="/services/ExpenseService1.svc/DeleteExpenseSheet",
            data=lambda: {
                "expenseSheetUri": rail.result('publish_expense_sheet_draft')['uri']
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> query_get_matching_records_from_valid_records_for_expense_sheet

        query_get_matching_records_from_valid_records_for_expense_sheet >> create_new_expense_sheet_draft >> update_expense_sheet_description \
            >> publish_expense_sheet_draft >> put_expense_sheet_entries >> submit_expense_sheet \
            >> add_expense_sheet_success_log_entry >> catch_and_log_error

        catch_and_log_error >> if_error_in_line_items

        if_error_in_line_items >> rail.Label('Yes') >> delete_expense_sheet

    return dag


rail.for_each_instance(create_child_dag)
