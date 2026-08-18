import rail


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'mmr_consulting_get_expenses_toupdate_expense_description_for_invoicesync_{config.instance}',
        description=f'Mmr_consulting_get_expenses_toupdate_expense_description_for_invoicesync {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'mmr_consulting_get_expenses_toupdate_expense_description_for_invoicesync_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_waiting_approvers = rail.RepliconServiceOperator(
            task_id='get_waiting_approvers',
            endpoint="services/ExpenseApprovalService1.svc/GetCurrentlyWaitingOnApprovers",
            data=lambda dag_run:
            {
                "expenseUri": dag_run.conf['webhook']['data']['expenseSheet']['uri']
            }
        )

        has_waiting_approver = rail.IfOperator(
            task_id='has_waiting_approver',
            test=lambda:  ('expense.processing, mmr' in [item['displayText'] for item in rail.result(
                'get_waiting_approvers') if 'displayText' in item.keys()]) if rail.result('get_waiting_approvers') else False,
            yes_task='get_expensesheet_details',
        )

        get_expensesheet_details = rail.RepliconServiceOperator(
            task_id='get_expensesheet_details',
            endpoint="services/ExpenseService1.svc/GetExpenseSheetDetails",
            data=lambda dag_run:
            {
                "expenseSheetUri": dag_run.conf['webhook']['data']['expenseSheet']['uri']
            }
        )

        if_expense_description_not_starts_with_expense_trackingno = rail.IfOperator(
            task_id='if_expense_description_not_starts_with_expense_trackingno',
            test="{{result('get_expensesheet_details')['description'] | starts_with(result('get_expensesheet_details')['trackingNumber']) | is_falsy}}",
            yes_task="reopen_expenses",
            no_task="approve_expenses",
        )

        reopen_expenses = rail.RepliconServiceOperator(
            task_id='reopen_expenses',
            endpoint="/services/ExpenseApprovalService1.svc/Reopen",
            data={
                "expenseUri": "{{result('get_expensesheet_details').uri}}",
                "unitOfWorkId": "Reopen_unitOfWorkId_{{result('get_expensesheet_details')['trackingNumber']}}_{{dag_run_ecid()}}",
                "comments": "Reopened by Replicon Integration"
            }

        )

        update_expense_sheet_description = rail.RepliconServiceOperator(
            task_id='update_expense_sheet_description',
            endpoint="/services/ExpenseService1.svc/UpdateExpenseSheetDescription",
            data={
                "expenseSheetUri": "{{result('get_expensesheet_details').uri}}",
                "description": "{{result('get_expensesheet_details')['trackingNumber']}} | {{result('get_expensesheet_details')['description']}}",
            }
        )

        submit_expense = rail.RepliconServiceOperator(
            task_id='submit_expense',
            endpoint="/services/ExpenseApprovalService1.svc/Submit",
            data={
                "expenseUri": "{{result('get_expensesheet_details').uri}}",
                "unitOfWorkId": "Submit_unitOfWorkId_{{result('get_expensesheet_details')['trackingNumber']}}_{{dag_run_ecid()}}",
                "comments": "Resubmitted by Replicon Integration"
            }
        )

        approve_expenses = rail.RepliconServiceOperator(
            task_id='approve_expenses',
            endpoint="/services/ExpenseApprovalService1.svc/Approve",
            data={
                "expenseUri": "{{result('get_expensesheet_details').uri}}",
                "unitOfWorkId": "Approve_unitOfWorkId_{{result('get_expensesheet_details')['trackingNumber']}}_{{dag_run_ecid()}}",
                "comments": "Approved by Replicon Integration"
            }
        )

        is_approve_expenses_already_approved = rail.IfOperator(
            task_id='is_approve_expenses_already_approved',
            trigger_rule='one_failed',
            test=lambda: rail.render_template(
            '{{result("approve_expenses", key="error")["response"]["json"]["error"]["details"]["notifications"][0]["displayText"]}}'
            ) == 'The item is not waiting for your approval.' if rail.render_template(
            '{{result("approve_expenses", key="error")["response"]["json"]["error"]["details"]["notifications"][0]["displayText"]}}'
            ) else False,
            yes_task='finish',
            no_task='fail_dagrun'
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_waiting_approvers >> has_waiting_approver >> get_expensesheet_details >> if_expense_description_not_starts_with_expense_trackingno
        if_expense_description_not_starts_with_expense_trackingno >> rail.Label(
            'Yes') >> reopen_expenses >> update_expense_sheet_description >> submit_expense
        if_expense_description_not_starts_with_expense_trackingno >> rail.Label(
            'No') >> approve_expenses >> is_approve_expenses_already_approved >> rail.Label(
            "No") >> fail_dagrun
        
        is_approve_expenses_already_approved >> rail.Label(
            "Yes") >> finish

        return dag


rail.for_each_instance(create_dag)
