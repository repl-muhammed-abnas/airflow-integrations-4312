import uuid
import rail
from techserv.expense_description_update.utils import python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'techserv_expense_description_update_master_{config.instance}',
        description=f'Techserv_expense_description_update_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'techserv_expense_description_update_master_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_expensesheet_details = rail.RepliconServiceOperator(
            task_id='get_expensesheet_details',
            endpoint="services/ExpenseService1.svc/GetExpenseSheetDetails",
            data=lambda dag_run:
            {
                "expenseSheetUri": dag_run.conf['webhook']['data']['expenseSheet']['uri']
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="services/UserService1.svc/GetUserDetails",
            data=
                {
                    "userUri": "{{ result('get_expensesheet_details')['owner']['uri']}}"
                }
        )

        create_expense_description = rail.PythonOperator(
            task_id='create_expense_description',
            python_callable=python_callable.get_user_defined_data
        )

        if_expense_description_not_equal_to_create_expense_description = rail.IfOperator(
            task_id='if_expense_description_not_equal_to_create_expense_description',
            test="{{result('get_expensesheet_details')['description'] != result('create_expense_description')}}",
            yes_task='reopen_expenses',
            no_task='finish'

        )
        reopen_expenses = rail.RepliconServiceOperator(
            task_id='reopen_expenses',
            endpoint="/services/ExpenseApprovalService1.svc/Reopen",
            data={
                "expenseUri": "{{result('get_expensesheet_details').uri}}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "To update Description field with Initials and Date"
            }

        )

        update_expense_sheet_description = rail.RepliconServiceOperator(
            task_id='update_expense_sheet_description',
            endpoint="/services/ExpenseService1.svc/UpdateExpenseSheetDescription",
            data={
                "expenseSheetUri": "{{result('get_expensesheet_details').uri}}",
                "description": "{{result('create_expense_description')}}",
            }
        )

        submit_expense = rail.RepliconServiceOperator(
            task_id='submit_expense',
            endpoint="/services/ExpenseApprovalService1.svc/Submit",
            data={
                "expenseUri": "{{result('get_expensesheet_details').uri}}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Description field updated with Initials and Date"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )


        get_expensesheet_details >> get_user_details >> create_expense_description >> if_expense_description_not_equal_to_create_expense_description
        if_expense_description_not_equal_to_create_expense_description >> rail.Label(
            'Yes') >> reopen_expenses >> update_expense_sheet_description >> submit_expense
        if_expense_description_not_equal_to_create_expense_description >> rail.Label(
            'No') >> finish
        return dag

rail.for_each_instance(create_dag)
