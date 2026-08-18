import rail

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dagid,
        description='Lanter Delivery Systems User Import - Disable User Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}'
            }
        )

        log_user_disabled = rail.WriteLogOperator(
            task_id="log_user_disabled",
            message="User Disabled Succesfully",
            severity='Success',
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Disable",
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Disable",
                'status': 'Error'
            },
        )

        disable_user >> log_user_disabled >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
