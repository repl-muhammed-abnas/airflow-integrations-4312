import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dagid,
        description='Accenture Payroll Integration MRDR Webhook Master',
        max_active_runs=config.max_active_runs,
        integration_type='generic',
        company_key=config.company_key,
        replicon_conn_id=None,
        webhook_conf=rail.WebhookConf(
            basic_auth_username_var=config.basic_auth_username_accenture_mrdr,
            basic_auth_password_var=config.basic_auth_password_accenture_mrdr
        ),
    ) as dag:

        view_dag_run_conf = rail.ViewDagRunConfOperator(task_id='view_dag_run_conf')

        def _get_child_conf(dag_run):
            data = dag_run.conf.get('webhook', {}).get('data', {})
            return {
                'PayrollID': data.get('PayrollID'),
                'PayrollFileID': data.get('PayrollFileID'),
                'PayrollFileName': data.get('PayrollFileName'),
                'vantagepoint_conn_id': config.vantagepoint_conn_id,
            }

        trigger_payroll_child = rail.TriggerDagRunOperator(
            task_id='trigger_payroll_child',
            trigger_dag_id=config.process_payroll_child_dag_id,
            conf=_get_child_conf,
        )

        view_dag_run_conf >> trigger_payroll_child

        return dag


rail.for_each_instance(create_main_dag)
